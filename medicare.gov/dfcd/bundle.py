""""""

from ambry.bundle.loader import CsvBundle


class Bundle(CsvBundle):

    """"""

    def line_mangler(self, source, l):

        return l.replace('\0', '')
