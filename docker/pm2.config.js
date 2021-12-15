module.exports = {
    apps: [
        {
            name: "jupyter-lab",
            script: "/usr/local/anaconda3/envs/cl/bin/jupyter-lab --ip=0.0.0.0 --port=8888 --allow-root --NotebookApp.token=262468670f9a00b51e3f93b0955a0bdfdcba7ba3e8b821c5 --notebook-dir=/root/app"
        },
    ]
}
