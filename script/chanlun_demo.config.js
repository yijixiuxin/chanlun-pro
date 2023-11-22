module.exports = {
    apps: [{
        name: "demo_web",
        cwd: "./../web/demo_web",
        script: "/root/miniconda3/envs/chanlun/bin/python3 manage.py runserver 0.0.0.0:8080 --noreload",
        error_file: "./logs/web-error.log",
        out_file: "./logs/web-out.log",
    }
    ]
}
