import {
  Bar,
  HistoryMetadata,
  LibrarySymbolInfo,
  PeriodParams,
} from "../../../charting_library/datafeed-api";

import {
  getErrorMessage,
  RequestParams,
  UdfErrorResponse,
  UdfOkResponse,
  UdfResponse,
} from "./helpers";

import { IRequester } from "./irequester";

// tslint:disable: no-any
interface HistoryPartialDataResponse extends UdfOkResponse {
  t: number[];
  c: number[];
  o?: never;
  h?: never;
  l?: never;
  v?: never;
  update?: boolean;
}

interface HistoryFullDataResponse extends UdfOkResponse {
  t: number[];
  c: number[];
  o: number[];
  h: number[];
  l: number[];
  v: number[];
  fxs: TextPoint[];
  bis: LineSegment[];
  xds: LineSegment[];
  zsds: LineSegment[];
  bi_zss: LineSegment[];
  xd_zss: LineSegment[];
  zsd_zss: LineSegment[];
  bcs: TextPoint[];
  mmds: TextPoint[];
  update: boolean;
  chart_color?: Map<string, string>;
}

// tslint:enable: no-any
interface HistoryNoDataResponse extends UdfResponse {
  s: "no_data";
  nextTime?: number;
}

type HistoryResponse =
  | HistoryFullDataResponse
  | HistoryPartialDataResponse
  | HistoryNoDataResponse;

export type PeriodParamsWithOptionalCountback = Omit<
  PeriodParams,
  "countBack"
> & { countBack?: number };

// 定义点位接口
interface Point {
  price: number;
  time: number;
}

// 定义带线型的线段接口
interface LineSegment {
  linestyle: string;
  points: Point[];
}

// 定义带文本的点位接口
interface TextPoint {
  points: Point | Point[];
  text: string;
}

export interface GetBarsResult {
  bars: Bar[];
  meta: HistoryMetadata;
  fxs: TextPoint[];
  bis: LineSegment[];
  xds: LineSegment[];
  zsds: LineSegment[];
  bi_zss: LineSegment[];
  xd_zss: LineSegment[];
  zsd_zss: LineSegment[];
  bcs: TextPoint[];
  mmds: TextPoint[];
  chart_color?: Map<string, string>;
}

export interface LimitedResponseConfiguration {
  /**
   * Set this value to the maximum number of bars which
   * the data backend server can supply in a single response.
   * This doesn't affect or change the library behavior regarding
   * how many bars it will request. It just allows this Datafeed
   * implementation to correctly handle this situation.
   */
  maxResponseLength: number;
  /**
   * If the server can't return all the required bars in a single
   * response then `expectedOrder` specifies whether the server
   * will send the latest (newest) or earliest (older) data first.
   */
  expectedOrder: "latestFirst" | "earliestFirst";
}

export class HistoryProvider {
  private _datafeedUrl: string;
  private readonly _requester: IRequester;
  private readonly _limitedServerResponse?: LimitedResponseConfiguration;
  public bars_result: Map<string, GetBarsResult>;

  public constructor(
    datafeedUrl: string,
    requester: IRequester,
    limitedServerResponse?: LimitedResponseConfiguration
  ) {
    this._datafeedUrl = datafeedUrl;
    this._requester = requester;
    this._limitedServerResponse = limitedServerResponse;
    this.bars_result = new Map();
  }

  public getBars(
    symbolInfo: LibrarySymbolInfo,
    resolution: string,
    periodParams: PeriodParamsWithOptionalCountback
  ): Promise<GetBarsResult> {
    const requestParams: RequestParams = {
      symbol: symbolInfo.ticker || "",
      resolution: resolution,
      from: periodParams.from,
      to: periodParams.to,
    };
    if (periodParams.countBack !== undefined) {
      requestParams.countback = periodParams.countBack;
    }

    if (periodParams.firstDataRequest !== undefined) {
      requestParams.firstDataRequest = periodParams.firstDataRequest;
    }

    if (symbolInfo.currency_code !== undefined) {
      requestParams.currencyCode = symbolInfo.currency_code;
    }

    if (symbolInfo.unit_id !== undefined) {
      requestParams.unitId = symbolInfo.unit_id;
    }

    return new Promise(
      async (
        resolve: (result: GetBarsResult) => void,
        reject: (reason: string) => void
      ) => {
        try {
          const initialResponse =
            await this._requester.sendRequest<HistoryResponse>(
              this._datafeedUrl,
              "history",
              requestParams
            );
          const result = this._processHistoryResponse(
            initialResponse,
            requestParams
          );

          if (this._limitedServerResponse) {
            await this._processTruncatedResponse(result, requestParams);
          }
          resolve(result);
        } catch (e: unknown) {
          if (e instanceof Error || typeof e === "string") {
            const reasonString = getErrorMessage(e);
            // tslint:disable-next-line:no-console
            console.warn(
              `HistoryProvider: getBars() failed, error=${reasonString}`
            );
            reject(reasonString);
          }
        }
      }
    );
  }

  private async _processTruncatedResponse(
    result: GetBarsResult,
    requestParams: RequestParams
  ) {
    let lastResultLength = result.bars.length;
    try {
      while (
        this._limitedServerResponse &&
        this._limitedServerResponse.maxResponseLength > 0 &&
        this._limitedServerResponse.maxResponseLength === lastResultLength &&
        requestParams.from < requestParams.to
      ) {
        // adjust request parameters for follow-up request
        if (requestParams.countback) {
          requestParams.countback =
            (requestParams.countback as number) - lastResultLength;
        }
        if (this._limitedServerResponse.expectedOrder === "earliestFirst") {
          requestParams.from = Math.round(
            result.bars[result.bars.length - 1].time / 1000
          );
        } else {
          requestParams.to = Math.round(result.bars[0].time / 1000);
        }

        const followupResponse =
          await this._requester.sendRequest<HistoryResponse>(
            this._datafeedUrl,
            "history",
            requestParams
          );
        const followupResult = this._processHistoryResponse(
          followupResponse,
          requestParams
        );
        lastResultLength = followupResult.bars.length;
        // merge result with results collected so far
        if (this._limitedServerResponse.expectedOrder === "earliestFirst") {
          if (
            followupResult.bars[0].time ===
            result.bars[result.bars.length - 1].time
          ) {
            // Datafeed shouldn't include a value exactly matching the `to` timestamp but in case it does
            // we will remove the duplicate.
            followupResult.bars.shift();
          }
          result.bars.push(...followupResult.bars);
        } else {
          if (
            followupResult.bars[followupResult.bars.length - 1].time ===
            result.bars[0].time
          ) {
            // Datafeed shouldn't include a value exactly matching the `to` timestamp but in case it does
            // we will remove the duplicate.
            followupResult.bars.pop();
          }
          result.bars.unshift(...followupResult.bars);
        }
      }
    } catch (e: unknown) {
      /**
       * Error occurred during followup request. We won't reject the original promise
       * because the initial response was valid so we will return what we've got so far.
       */
      if (e instanceof Error || typeof e === "string") {
        const reasonString = getErrorMessage(e);
        // tslint:disable-next-line:no-console
        console.warn(
          `HistoryProvider: getBars() warning during followup request, error=${reasonString}`
        );
      }
    }
  }

  private _processHistoryResponse(
    response: HistoryResponse | UdfErrorResponse,
    requestParams: RequestParams
  ) {
    if (response.s !== "ok" && response.s !== "no_data") {
      throw new Error(response.errmsg);
    }

    const bars: Bar[] = [];
    const meta: HistoryMetadata = {
      noData: false,
    };

    if (response.s === "no_data") {
      meta.noData = true;
      meta.nextTime = response.nextTime;
    } else {
      const volumePresent = response.v !== undefined;
      const ohlPresent = response.o !== undefined;

      for (let i = 0; i < response.t.length; ++i) {
        const barValue: Bar = {
          time: response.t[i] * 1000,
          close: response.c[i],
          open: response.c[i],
          high: response.c[i],
          low: response.c[i],
        };

        if (ohlPresent) {
          barValue.open = (response as HistoryFullDataResponse).o[i];
          barValue.high = (response as HistoryFullDataResponse).h[i];
          barValue.low = (response as HistoryFullDataResponse).l[i];
        }

        if (volumePresent) {
          barValue.volume = (response as HistoryFullDataResponse).v[i];
        }

        bars.push(barValue);
      }

      // 设置保存的key
      const res_key: string =
        requestParams["symbol"].toString().toLowerCase() +
        requestParams["resolution"].toString().toLowerCase();

      // 保存数据
      let obj_res = this.bars_result.get(res_key);
      if (response.update == false || obj_res == undefined) {
        this.bars_result.set(res_key, {
          bars: bars,
          meta: meta,
          fxs: (response as HistoryFullDataResponse).fxs,
          bis: (response as HistoryFullDataResponse).bis,
          xds: (response as HistoryFullDataResponse).xds,
          zsds: (response as HistoryFullDataResponse).zsds,
          bi_zss: (response as HistoryFullDataResponse).bi_zss,
          xd_zss: (response as HistoryFullDataResponse).xd_zss,
          zsd_zss: (response as HistoryFullDataResponse).zsd_zss,
          bcs: (response as HistoryFullDataResponse).bcs,
          mmds: (response as HistoryFullDataResponse).mmds,
          chart_color: (response as HistoryFullDataResponse).chart_color,
        });
      } else {
        // 更新存在的数据
        // 更新逻辑，找到大于等于返回的第一个时间的所有数据；
        // 保留小于返回的第一个时间的所有数据；
        // 然后添加返回的数据；
        // 最后按时间排序；

        // 1. 更新其他数据结构（如分型、笔、线段等）
        // 处理TextPoint类型数据（fxs, bcs, mmds）
        const updateTextPoints = (
          existingPoints: TextPoint[],
          newPoints: TextPoint[]
        ): TextPoint[] => {
          if (!newPoints || newPoints.length === 0) return existingPoints || [];
          if (!existingPoints || existingPoints.length === 0) return newPoints;

          // 获取点位时间的辅助函数，处理points可能是对象或数组的情况
          const getPointTime = (point: TextPoint): number => {
            if (Array.isArray(point.points)) {
              // 如果是数组，取第一个元素的time
              return point.points[0].time;
            } else {
              // 如果是单个对象，直接取time
              return point.points.time;
            }
          };

          const minResponseTime = Math.min(...newPoints.map(getPointTime));
          const updatedPoints: TextPoint[] = [];
          // 保留小于最小时间点的数据
          for (const point of existingPoints) {
            if (getPointTime(point) < minResponseTime) {
              updatedPoints.push(point);
            }
          }
          // 添加返回数据中剩余的新点位
          for (const point of newPoints) {
            updatedPoints.push(point);
          }
          // 按时间排序，使用getPointTime辅助函数获取时间
          return updatedPoints.sort(
            (a, b) => getPointTime(a) - getPointTime(b)
          );
        };

        // 处理LineSegment类型数据（bis, xds, zsds, bi_zss, xd_zss, zsd_zss）
        const updateLineSegments = (
          existingSegments: LineSegment[],
          newSegments: LineSegment[]
        ): LineSegment[] => {
          if (!newSegments || newSegments.length === 0)
            return existingSegments || [];
          if (!existingSegments || existingSegments.length === 0)
            return newSegments;

          const minResponseTime = Math.min(
            ...newSegments.map((segment) => segment.points[0].time)
          );

          const updatedSegments: LineSegment[] = [];

          // 保留起始时间小于最小时间点的线段
          for (const segment of existingSegments) {
            if (segment.points.length > 0) {
              if (segment.points[0].time < minResponseTime) {
                updatedSegments.push(segment);
              }
            }
          }

          // 添加返回数据中剩余的新线段
          for (const segment of newSegments) {
            updatedSegments.push(segment);
          }

          // 按起始时间排序
          return updatedSegments.sort((a, b) => {
            if (a.points.length === 0 && b.points.length === 0) return 0;
            if (a.points.length === 0) return -1;
            if (b.points.length === 0) return 1;
            return a.points[0].time - b.points[0].time;
          });
        };

        // 更新所有数据
        obj_res.fxs = updateTextPoints(
          obj_res.fxs,
          (response as HistoryFullDataResponse).fxs
        );
        obj_res.bis = updateLineSegments(
          obj_res.bis,
          (response as HistoryFullDataResponse).bis
        );
        obj_res.xds = updateLineSegments(
          obj_res.xds,
          (response as HistoryFullDataResponse).xds
        );
        obj_res.zsds = updateLineSegments(
          obj_res.zsds,
          (response as HistoryFullDataResponse).zsds
        );
        obj_res.bi_zss = updateLineSegments(
          obj_res.bi_zss,
          (response as HistoryFullDataResponse).bi_zss
        );
        obj_res.xd_zss = updateLineSegments(
          obj_res.xd_zss,
          (response as HistoryFullDataResponse).xd_zss
        );
        obj_res.zsd_zss = updateLineSegments(
          obj_res.zsd_zss,
          (response as HistoryFullDataResponse).zsd_zss
        );
        obj_res.bcs = updateTextPoints(
          obj_res.bcs,
          (response as HistoryFullDataResponse).bcs
        );
        obj_res.mmds = updateTextPoints(
          obj_res.mmds,
          (response as HistoryFullDataResponse).mmds
        );
        obj_res.chart_color = (response as HistoryFullDataResponse).chart_color;
        this.bars_result.set(res_key, obj_res);
      }
    }

    const result = {
      bars: bars,
      meta: meta,
      fxs: (response as HistoryFullDataResponse).fxs,
      bis: (response as HistoryFullDataResponse).bis,
      xds: (response as HistoryFullDataResponse).xds,
      zsds: (response as HistoryFullDataResponse).zsds,
      bi_zss: (response as HistoryFullDataResponse).bi_zss,
      xd_zss: (response as HistoryFullDataResponse).xd_zss,
      zsd_zss: (response as HistoryFullDataResponse).zsd_zss,
      bcs: (response as HistoryFullDataResponse).bcs,
      mmds: (response as HistoryFullDataResponse).mmds,
      chart_color: (response as HistoryFullDataResponse).chart_color,
    };

    return result;
  }
}
