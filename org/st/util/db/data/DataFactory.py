# coding=utf-8

"""
所有数据工厂类的基类, 定义了数据库配置信息操作常用的变量和函数
"""
class BaseDataFactory:
    def createData(self, dataClass, engine):
        """
        创建生成数据操作类对象实例
        :param dataClass: class, 数据操作类
        :return: instance, 数据操作类实例
        """
        return dataClass(engine)