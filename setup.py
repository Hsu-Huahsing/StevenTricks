from setuptools import find_packages, setup
from os import environ

setup(
    name='StevenTricks',  # 应用名
    author=environ['USER'],
    version='0.1.2',  # 版本号
    packages=find_packages(),  # 包括在安装包内的Python包
    # include_package_data=True,  # 启用清单文件MANIFEST.in,包含数据文件
    exclude_package_data={'': ['.gitignore']},  # 上面的代码会将所有”.gitignore”文件排除在包外。如果上述exclude_package_date对象属性不为空，比如{'myapp':['.gitignore']}，就表明只排除”myapp”包下的所有”.gitignore”文件。
    # {'docs': ['']},
    # install_requires=[  # 自动安装依赖
    #     # 'Flask>=0.10',
    # ],
)