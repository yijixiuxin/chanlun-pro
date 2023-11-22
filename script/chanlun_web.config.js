module.exports = {
    apps: [{
        name: "chanlun_web",
        cwd: "./../web/chanlun_web",
        script: "/root/miniconda3/envs/chanlun/bin/python3 manage.py runserver 0.0.0.0:8000 --noreload",
        error_file: "./logs/web-error.log",
        out_file: "./logs/web-out.log",
    }
    ]
}
