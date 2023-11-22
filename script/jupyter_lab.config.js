module.exports = {
    apps: [{
        name: "jupyter_lab",
        cwd: "./../",
        script: "/root/miniconda3/envs/chanlun/bin/python3 jupyter-lab",
        error_file: "./logs/jupyter-lab-error.log",
        out_file: "./logs/jupyter-lab-out.log",
    },]
}
