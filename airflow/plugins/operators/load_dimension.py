from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info('... starting load dimension')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.table,
            self.sql
        )
        
        redshift.run(formatted_sql)
