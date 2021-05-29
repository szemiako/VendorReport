'''Module to build query templates and execute them.'''

class QueriesFactory:
    '''Factory method to create new query templates. Can instantiate it
    once and then get templates later.'''
    def __init__(
        self,
    ):
        self.templates = self.build_templates()

    def register_template(self, templates: dict, key: str, template) -> dict:
        '''Add new template to templates dict.'''
        templates[key] = template

    def build_templates(self, templates: dict={}) -> dict:
        '''Register all existing templates.'''
        self.register_template(templates, 'configs', Configs)
        self.register_template(templates, 'expected', Expected)
        self.register_template(templates, 'filemask', Filemask)
        return templates

    def get_template(self, key: str):
        '''Get the template based on the key (keys defined in
        build_templates()).'''
        template = self.templates.get(key)
        if not template:
            return key
        else:
            return template()

class Queries:
    def init_temp_table(self) -> str:
        '''Initialize the temp table with schema.'''
        ...

    def insert_values(self, sql: str='') -> str:
        '''Insert the values into the temp table.'''
        ...

    def query_template(self) -> str:
        '''Execute the temp table query.'''
        ...
    
    def execute_sql(self, conn,  file_mask, template) -> list:
        '''Initialize a temp table, insert values, and execute query.'''
        conn._execute(template.init_temp_table())
        conn._execute(template.insert_values(file_mask))
        return conn._execute_select_all(template.query_template())

class Configs(Queries):
    def init_temp_table(self) -> str:
        return '''
            IF (Object_ID('tempdv..#configs') IS NOT NULL)
            BEGIN
                DROP TABLE #configs
            END

            CREATE TABLE #configs (
                vendor_name VARCHAR(MAX),
                name VARCHAR(MAX),
                offset INT
            )
        '''

    def insert_values(self, configs: dict, sql: str='') -> str:
        values = '''INSERT INTO #configs (
            vendor_name,
            name,
            offset
        ) VALUES (
            '{vendor_name}',
            '{name}',
            '{offset}'
        )'''

        for i in configs:
            sql += values.format(
                vendor_name=i,
                name=configs[i]['name'],
                offset=int(configs[i]['offset'])
            )
        return sql

    def query_template(self) -> str:
        return '''
            select
                conf.vendor_name,
                conf.name,
                conf.offset,
                v.number
            from #configs conf
            join [vendors] v on conf.vendor_name = v.name
        '''

class Expected(Queries):    
    def init_temp_table(self) -> str:
        return '''
            IF (Object_ID('tempdv..#expected') IS NOT NULL)
            BEGIN
                DROP TABLE #expected
            END

            CREATE TABLE #expected (
                vendor_name VARCHAR(MAX),
                name VARCHAR(MAX),
                offset INT,
                number INT,
                feed_type VARCHAR(MAX)
            )
        '''

    def insert_values(self, filemask: dict, sql: str='') -> str:
        values = '''INSERT INTO #expected (
            vendor_name,
            name,
            offset,
            number,
            feed_type
        ) VALUES (
            '{vendor_name}',
            '{name}',
            '{offset}',
            '{number}',
            '{feed_type}'
        )'''

        for i in filemask:
            sql += values.format(
                vendor_name=filemask[i]['vendor_name'],
                name=filemask[i]['name'],
                offset=int(filemask[i]['offset']),
                number=int(filemask[i]['number']),
                feed_type=filemask[i]['feed_type']
            )
        return sql

    def query_template(self) -> str:
        return '''
            select
                v.name,
                c.name,
                fic.mask,
                ft.name,
                exp.offset,
                exp.number,
                exp.feed_type
            from #expected exp
            join [vendors] v on exp.vendor_name = v.name
            join [feed_config] fec on v.id = fec.vendor_id
            join [file_config] fic on fec.id = fic.config_id
            join [feed_types] ft on fic.type_id = ft.id
            join [crm] c on fec.crm_id = c.id
        '''

class Filemask(Queries):
    def init_temp_table(self) -> str:
        return '''
            IF (Object_ID('tempdv..#filemask') IS NOT NULL)
            BEGIN
                DROP TABLE #filemask
            END

            CREATE TABLE #filemask (
                file_mask VARCHAR(MAX),
                vendor_name VARCHAR(MAX),
                name VARCHAR(MAX),
                number INT,
                feed_type VARCHAR(MAX)
            )
        '''

    def insert_values(self, filemask: dict, sql: str='') -> str:
        values = '''INSERT INTO #filemask (
            file_mask,
            vendor_name,
            name,
            number,
            feed_type
        ) VALUES (
            '{FileMask}',
            '{vendor_name}',
            '{name}',
            '{number}',
            '{feed_type}'
        )'''

        for i in filemask:
            sql += values.format(
                FileMask=filemask[i]['mask'],
                vendor_name=filemask[i]['vendor_name'],
                name=filemask[i]['name'],
                number=int(filemask[i]['number']),
                feed_type=filemask[i]['feed_type']
            )

        return sql

    def query_template(self) -> str:
        return '''
            select
                masks. file_mask,
                masks.number,
                masks.feed_type,
                log.FileName,
                log.name,
                log.download,
                log.archive_time,
                log.start,
                log.end,
                log.vendor_name,
                log.name
            from #filemask masks
            cross apply (
                select top (1)
                    log.FileName,
                    ft.name,
                    log.download,
                    log.archive_time,
                    log.start,
                    log.end,
                    v.name,
                    c.name
                from [process_log] log
                join [file_config] fic on log.feed_id = fic.id
                join [feed_types] ft on fic.type_id = ft.id
                join [feed_config] fec on fic.config_id = fec.id
                join [vendors] v ON fec.vendor_id = v.id
                join [crm] c ON fec.id = c.id
                where
                    log.filename like '%' + masks.file_mask + '%'
                    AND v.name = masks.vendor_name
                order by log.download desc
            ) log
        '''