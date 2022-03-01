import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="chanlun",
    version="0.0.1",
    author="Wang Xu",
    author_email="1058715329@qq.com",
    description="基于缠论对过往行情数据进行分析的工具包。",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/yijixiuxin/chanlun",
    project_urls={
        "Bug Tracker": "https://github.com/yijixiuxin/chanlun/issues",
    },
    install_requires=[
        "numpy", "pandas", "pyecharts", "TA-Lib"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    package_dir={'chanlun': 'src/chanlun'},
    packages=['chanlun'],
    python_requires=">=3.6",
)
