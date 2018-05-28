class all_hosts_container(object):
    _ids = '05 06 07 s03 s04 s05 s06 s07 s08 s09'

    @classmethod
    def get_host(cls, id_):
        if id_.startswith('s'):
            host = 'server{}'.format(id_[len('s'):])
        else:
            host = 'gserver{}'.format(id_)

        return host

    @classmethod
    def get_hosts(cls, *selectors):
        is_choosen = None if 'all' in selectors else set(selectors).__contains__
        ids = filter(is_choosen, cls._ids.split(' '))
        hosts = map(cls.get_host, ids)

        exclude_host_list = []
        exclude_selector_list = [selector[len('x'):] for selector in selectors if selector.startswith('x')]
        if exclude_selector_list:
            is_choosen = set(exclude_selector_list).__contains__
            exclude_id_list = filter(is_choosen, cls._ids.split(' '))
            exclude_host_list = map(cls.get_host, exclude_id_list)

        result = sorted(list(set(hosts) - set(exclude_host_list)))
        return result
