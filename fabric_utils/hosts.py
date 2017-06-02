class all_hosts_container(object):
    _ids = '01 05 06 07 s01 s02 s03 s04 s05 s06 s07 s08 s09 s10'

    @classmethod
    def get_host(cls, id_):
        if id_.startswith('s'):
            host = 'server{}'.format(id_[len('s'):])
        else:
            host = 'gserver{}'.format(id_)

        return host

    @classmethod
    def get_hosts(cls, *selectors):
        is_choosen = None if selectors == ('all', ) else set(selectors).__contains__
        ids = filter(is_choosen, cls._ids.split(' '))
        hosts = map(cls.get_host, ids)

        return hosts
