# coding=utf-8


class LazyProvider(object):
    """Обертка над функциями, которые обращаются к внешним системам.

    Предоставляет единообразный интерфейс для отложенного выполнения запроса.
    """

    def __init__(self, provider):
        self.called_once = False
        self.provider = provider
        self.value = None

    def provide_content(self):
        if not self.called_once:
            self.value = self.provider()
            self.called_once = True

        return self.value
