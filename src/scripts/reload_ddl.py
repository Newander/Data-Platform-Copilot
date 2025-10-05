from src.database.base_model import create_all
from src.database.db_connector import create_connection, ConnectionCM

if __name__ == '__main__':
    db_connection = create_connection()
    db_connection.test_db_connection()
    create_all(ConnectionCM(db_connection), with_drop=True)
