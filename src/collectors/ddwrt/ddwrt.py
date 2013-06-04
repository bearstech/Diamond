# coding=utf-8


import urllib2
import diamond.collector


class DD(object):

    def __init__(self, domain, user, password):
        if domain[-1] != '/':
            domain = '%s/' % domain
        self.domain = domain
        self.user = user
        self.password = password
        self.data = {}

    def fetch(self, page):
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, self.domain, self.user, self.password)
        urllib2.install_opener(
            urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passman)))
        req = urllib2.Request("%s%s.live.asp" % (self.domain, page))
        response = urllib2.urlopen(req)
        assert response.getcode() == 200
        for line in response:
            keyvalue = line[1:-1].split('::', 2)
            if keyvalue != ['']:
                yield keyvalue

    def clear(self):
        self.data = {}

    def refresh(self, page):
        self.data[page] = {}
        for key, value in self.fetch(page):
            self.data[page][key] = value

    def wireless_clients(self):
        d = self.data['Status_Wireless']['active_wireless'][1:-1].split("','")
        for a in range(0, len(d) - 9, 9):
            (mac, interface, uptime, tx, rx,
             signal, noise, snr, stuff) = d[a:a + 9]
            signal = ununit(unquote(signal))
            noise = ununit(unquote(noise))
            snr = unquoteint(snr)
            tx = ununit(unquote(tx))
            rx = ununit(ununit(rx))
            yield unquote(mac), {
                'if': unquote(interface),
                'uptime': unquote(uptime),
                'tx': tx,
                'rx': rx,
                'signal': signal,
                'noise': noise,
                'SNR': snr,
                'quality': round(signal * 1.0 / noise * snr, 1)
            }


def unquote(txt):
    if txt[0] == "'":
        start = 1
    else:
        start = 0
    if txt[-1] == "'":
        return txt[start:-1]
    else:
        return txt[start:]


def unquoteint(txt):
    return int(unquote(txt))


def ununit(txt):
    if txt is None or txt == 'N/A':
        return None
    try:
        return int(txt)
    except ValueError:
        if txt[-1] == 'K':
            return int(txt[:-1]) * 1000
        if txt[-1] == 'M':
            return int(txt[:-1]) * 1000000
        if txt[-1] == 'G':
            return int(txt[:-1]) * 1000000000
        raise Exception("Unknown unit : %s" % txt[-1])


class DDWRTCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(DDWRTCollector,
                            self).get_default_config_help()
        config_help.update({
            'domain': "",
            'password': "",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(DDWRTCollector, self).get_default_config()
        config.update({
            'domain':     'http://127.0.0.1',
            'password':   'foo'
        })
        return config

    def collect(self):
        dd = DD(self.config['domain'], 'admin', self.config['password'])
        dd.refresh('Status_Wireless')
        for mac, values in dd.wireless_clients():
            for key in ['noise', 'SNR', 'tx', 'rx', 'quality', 'signal']:
                stat_name = '%s.%s' % (mac, key)
                try:
                    stat_value = float(values[key])
                except TypeError:
                    continue
                self.publish(stat_name, stat_value, metric_type='GAUGE')

if __name__ == '__main__':
    import sys
    domain = sys.argv[1]
    passwd = sys.argv[2]
    dd = DD(domain, 'admin', passwd)
    dd.refresh('Status_Wireless')
    print(list(dd.wireless_clients()))
