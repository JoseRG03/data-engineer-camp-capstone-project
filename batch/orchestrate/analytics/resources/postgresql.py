from dagster import ConfigurableResource
from sqlalchemy import create_engine

class PostgresqlDatabaseResource(ConfigurableResource):
    host_name: str
    database_name: str
    username: str
    password: str
    port: str

    def get_engine(self):
            return create_engine(
                f"postgresql+psycopg2://{self.username}:{self.password}"
                f"@{self.host_name}:{self.port}/{self.database_name}"
            )