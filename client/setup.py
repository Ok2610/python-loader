from setuptools import setup

setup(
    name="data-loader_cli",
    version="0.1",
    py_modules=["cli","grpc_client","dataloader_pb2","dataloader_pb2_grpc","filemgmt"],
    include_package_data=True,
    install_requires=["click","grpcio"],
    entry_points='''
        [console_scripts]
        loader=cli:cli
    ''',
)