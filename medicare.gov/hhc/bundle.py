""""""

from ambry.bundle.loader import CsvBundle


class Bundle(CsvBundle):

    """"""

    def __init__(self, directory=None):

        super(Bundle, self).__init__(directory)

    def build(self):

        return True

    def meta(self):
        from collections import defaultdict
        from ambry.util.intuit import Intuiter
        import urllib2
        import unicodecsv as csv
        import os
        import uuid
        import zipfile

        # A proto terms map, for setting grains
        pt = self.library.get('civicknowledge.com-proto-proto_terms').partition

        self.database.create()

        self.resolve_sources()

        tables = defaultdict(set)

        # First, load in the protoschema, to get prefix columns for each table.
        sf_path = self.filesystem.path('meta', self.PROTO_SCHEMA_FILE)

        if os.path.exists(sf_path):
            with open(sf_path, 'rbU') as f:
                self.schema.schema_from_file(f)

        if not self.run_args.get('clean', None):
            self._prepare_load_schema()

        url = self.metadata.build.download_url
        zip_file = self.filesystem.download(url)

        cache = self.filesystem.get_cache_by_name('extracts')
        tmpdir = os.path.join(cache.cache_dir, 'tmp', str(uuid.uuid4()))

        with zipfile.ZipFile(zip_file, "r") as zf:
             zf.extractall(tmpdir)

        fn = self.filesystem.unzip(zip_file)

        # Collect all of the sources for each table, while also creating the tables

        for source_name, source in self.metadata.sources.items():

            table = self.make_table_for_source(source_name)
            tables[table.name].add(source_name)

        self.schema.write_schema()

        intuiters = defaultdict(Intuiter)

        # Intuit all of the tables

        for table_name, sources in tables.items():

            intuiter = intuiters[table_name]

            iterables = []

            for source_name in sources:
                self.log("Intuiting {} into {}".format(source_name, table_name))

                rg = self.row_gen_for_source(source_name)
                rg.file_name = os.path.join(tmpdir, rg.file_name)

                iterables.append(rg)

                intuiter.iterate(rg, 5000)

            self.schema.update_from_intuiter(table_name, intuiter)

            # Write the first 50 lines of the csv file, to see what the intuiter got from the
            # raw-row-gen
            with open(self.filesystem.build_path('{}-raw-rows.csv'.format(table_name)), 'w') as f:
                rg = self.row_gen_for_source(source_name)
                rg.file_name = os.path.join(tmpdir, rg.file_name)
                rrg = rg.raw_row_gen
                w = csv.writer(f)

                for i, row in enumerate(rrg):
                    if i > 100:
                        break

                    w.writerow(list(row))

            # Now write the first 50 lines from the row gen, after appliying the row spec
            with open(self.filesystem.build_path('{}-specd-rows.csv'.format(table_name)), 'w') as f:
                rg = self.row_gen_for_source(source_name)
                rg.file_name = os.path.join(tmpdir, rg.file_name)

                w = csv.writer(f)

                w.writerow(rg.header)

                for i, row in enumerate(rg):
                    if i > 100:
                        break

                    w.writerow(list(row))

            # Write an intuiter report, to review how the intuiter made it's decisions
            with open(self.filesystem.build_path('{}-intuit-report.csv'.format(table_name)), 'w') as f:
                w = csv.DictWriter(f, ("name length resolved_type has_codes count ints "
                                       "floats strs nones datetimes dates times strvals".split()))
                w.writeheader()
                for d in intuiter.dump():
                    w.writerow(d)

            # Load and update the column map
            # .. already loaded in the constructor

            # Update

            if os.path.exists(self.col_map_fn):
                col_map = self.filesystem.read_csv(self.col_map_fn, key='header')
            else:
                col_map = {}

            # Don't add the columns that are already mapped.
            mapped_domain = set(item['col'] for item in col_map.values())

            rg = self.row_gen_for_source(source_name)
            rg.file_name = os.path.join(tmpdir, rg.file_name)

            header = rg.header  # Also sets unmangled_header

            descs = [x.replace('\n', '; ') for x in (rg.unmangled_header if rg.unmangled_header else header)]

            for col_name, desc in zip(header, descs):
                k = col_name.strip()

                if k not in col_map and col_name not in mapped_domain:
                    col_map[k] = dict(header=k, col='')

            # Write back out
            with open(self.col_map_fn, 'w') as f:

                w = csv.DictWriter(f, fieldnames=['header', 'col'])
                w.writeheader()
                for k in sorted(col_map.keys()):
                    w.writerow(col_map[k])

        return True
