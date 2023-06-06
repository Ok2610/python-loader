from setuptools import setup

setup(
    name="data-loader_cli",
    version="0.1",
    py_modules=["app","medias_pb2","medias_pb2_grpc"],
    include_package_data=True,
    install_requires=["click","grpcio"],
    entry_points='''
        [console_scripts]
        loader=app:cli
    ''',
)