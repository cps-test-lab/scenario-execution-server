from setuptools import setup

PACKAGE_NAME = 'scenario_execution_server_ros'

setup(
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + PACKAGE_NAME]),
        ('share/' + PACKAGE_NAME, ['package.xml']),
    ],
)
