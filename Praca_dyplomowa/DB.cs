using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Praca_dyplomowa
{
    public class DB
    {
        public string GetDbConfig()
        {
            var connectionString = String.Format("server={0};uid={1};pwd={2};database={3}","127.0.0.1", "root", "Brzuch_1986", "pawel");
            return connectionString;
        }//metoda do zwracajaca konfiguracje bazy danych 
        
        public  void ExecuteMysqlQuerry(string querry)
        {

            var connectionString = GetDbConfig();
            using (var conn = new MySqlConnection(connectionString))
            {
                var command = new MySqlCommand(querry, conn)
                {
                    CommandType = CommandType.Text,
                    CommandTimeout = 300000
                };
                try
                {
                    conn.Open();
                    command.ExecuteNonQuery();
                    command.Dispose();
                    conn.Close();
                    conn.Dispose();
                }
                catch (Exception ex)
                {
                }
            }
        }

        public DataTable GetDatatableFromMySql(string query)
        {
            var connectionString = GetDbConfig();
            using (var conn = new MySqlConnection(connectionString))
            {
                var command = new MySqlCommand(query, conn)
                {
                    CommandType = CommandType.Text,
                    CommandTimeout = 300000
                };
                try
                {
                    conn.Open();
                    var dt = new DataTable();
                    dt.Load(command.ExecuteReader());
                    command.Dispose();
                    conn.Close();
                    conn.Dispose();
                    return dt;
                }
                catch (Exception e)
                {
                    return null;
                }
            }
        }

    }
}
