using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Praca_dyplomowa
{
    class Program
    {
        static void Main(string[] args)
        {
            var db = new DB();
            long number = 60;///określa ilośc literacji pentli

            CalculateSynchMulti(number);
            
            CalculateSynchSingle(number);

            CalculateAsync(number).Wait();


        }

        
        static void CalculateSynchMulti(long number)
        {
            var db = new DB();
            var timerbase = new Stopwatch();
            Console.WriteLine($"Rozpoczynam kopiowanie danych do DataTable. Czas start.");
            timerbase.Start();

            //Pobranie poszczegolnych tabel jako DataTable do pamięci komputera
            DataTable names = db.GetDatatableFromMySql("SELECT * FROM pawel.imiona;");
            DataTable surname = db.GetDatatableFromMySql("SELECT * FROM pawel.surname;");
            DataTable age = db.GetDatatableFromMySql("SELECT * FROM pawel.wiek;");
            DataTable rand_num = db.GetDatatableFromMySql("SELECT * FROM pawel.rand_numbers");
            DataTable city = db.GetDatatableFromMySql("SELECT nazwa_miasta FROM pawel.wojewodztwa_miasta");
            DataTable state = db.GetDatatableFromMySql("SELECT nazwa_wojewodztwa FROM pawel.wojewodztwa_miasta");
            string sql = "";
            timerbase.Stop();
            Console.WriteLine($"Baze wczytałem w  {timerbase.ElapsedMilliseconds / 1000}s.");

            //SYNCHRONICZNIE - wysyłamy do bazy jednorazowy obiekt zawierający 10 rekordow - ustanawiamy jedno polaczenie z dlugim czasem timeouta
            var timerloop = new Stopwatch();
            Console.WriteLine($"Rozpoczynam pętle tworzącą jedno zapytanie składające się z {number} rekordów. Czas start.");
            timerloop.Start();

            for (int i = 0; i < number; i++)//pętla tworząca jedno zapytanie
            {
                var rand = new Random();
                var name = names.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var sur = surname.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var age1 = age.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var rand_nums = rand_num.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var city1 = city.AsEnumerable().Select(x => x.ItemArray[0]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var state1 = state.AsEnumerable().Select(x => x.ItemArray[0]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();

                sql += $"INSERT INTO `pawel`.`synchroniczny`(`name`,`surname`,`wiek`,`miasto`,`wojewodztwo`,`random_num`) VALUES ('{name}', '{sur}', '{age1}', '{city1}', '{state1}','{rand_nums}');"; //dodajemy do zmiennej sql kazdorazowo nowa linijke tekstu

            }

            db.ExecuteMysqlQuerry(sql);
            timerloop.Stop();
            Console.WriteLine($"Pętle skąldająca się z {number} zapytań utworzyłem w   {timerloop.ElapsedMilliseconds / 1000}s.");
        }
        
        static void CalculateSynchSingle(long number)
        {
            var db = new DB();
            var timerbase = new Stopwatch();
            Console.WriteLine($"Rozpoczynam kopiowanie danych do DataTable. Czas start.");
            timerbase.Start();

            //Pobranie poszczegolnych tabel jako DataTable do pamięci komputera
            DataTable names = db.GetDatatableFromMySql("SELECT * FROM pawel.imiona;");
            DataTable surname = db.GetDatatableFromMySql("SELECT * FROM pawel.surname;");
            DataTable age = db.GetDatatableFromMySql("SELECT * FROM pawel.wiek;");
            DataTable rand_num = db.GetDatatableFromMySql("SELECT * FROM pawel.rand_numbers");
            DataTable city = db.GetDatatableFromMySql("SELECT nazwa_miasta FROM pawel.wojewodztwa_miasta");
            DataTable state = db.GetDatatableFromMySql("SELECT nazwa_wojewodztwa FROM pawel.wojewodztwa_miasta");
            string sql = "";
            timerbase.Stop();
            Console.WriteLine($"Baze wczytałem w  {timerbase.ElapsedMilliseconds / 1000}s.");


            // imiona 18622
            // surname 299007
            // wiek 9982806
            // rand num 9982368
            // miasto i wojewodztwo 918


            //SYNCHRONICZNE - wysyłamy kazdy element zbudowanego zapytania indywidualnie do bazy, za kazdym razem otwierajac nowe polaczenie
            var timerloop2 = new Stopwatch();
            Console.WriteLine($"Rozpoczynam pętle tworzącą {number} pojedyńczo wysyłanych do bazy rekordów. Czas start.");
            timerloop2.Start();

            for (int i = 0; i < number; i++)//pętla tworząca jedno zapytanie
            {
                var rand = new Random();
                var name = names.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var sur = surname.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var age1 = age.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var rand_nums = rand_num.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var city1 = city.AsEnumerable().Select(x => x.ItemArray[0]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
                var state1 = state.AsEnumerable().Select(x => x.ItemArray[0]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();

                sql = $"INSERT INTO `pawel`.`synchroniczny`(`name`,`surname`,`wiek`,`miasto`,`wojewodztwo`,`random_num`) VALUES ('{name}', '{sur}', '{age1}', '{city1}', '{state1}','{rand_nums}');"; //dodajemy do zmiennej sql kazdorazowo nowa linijke tekstu

                db.ExecuteMysqlQuerry(sql);

            }
            timerloop2.Stop();
            Console.WriteLine($"Wysłałem {number} pojedyńczych rekordów do bazy w   {timerloop2.ElapsedMilliseconds / 1000}s.");


        }

        private static async Task CalculateAsync(long number)
        {
            var db = new DB();
            var timerbase = new Stopwatch();
            Console.WriteLine($"Rozpoczynam kopiowanie danych do DataTable. Czas start.");
            timerbase.Start();

            //Pobranie poszczegolnych tabel jako DataTable do pamięci komputera
            DataTable names = db.GetDatatableFromMySql("SELECT * FROM pawel.imiona;");
            DataTable surname = db.GetDatatableFromMySql("SELECT * FROM pawel.surname;");
            DataTable age = db.GetDatatableFromMySql("SELECT * FROM pawel.wiek;");
            DataTable rand_num = db.GetDatatableFromMySql("SELECT * FROM pawel.rand_numbers");
            DataTable city = db.GetDatatableFromMySql("SELECT nazwa_miasta FROM pawel.wojewodztwa_miasta");
            DataTable state = db.GetDatatableFromMySql("SELECT nazwa_wojewodztwa FROM pawel.wojewodztwa_miasta");

            timerbase.Stop();
            Console.WriteLine($"Baze wczytałem w  {timerbase.ElapsedMilliseconds / 1000}s.");

            var timerloop3 = new Stopwatch();
            Console.WriteLine($"Rozpoczynam pętle tworzącą jedno zapytanie składające się z {number} przy wykonaniu zadania asynchronicznie. Czas start.");
            timerloop3.Start();

            var tasks = new List<Task>();//określenie listy zadań
            for (int i = 0; i < number; i++)//pętla tworząca jedno zapytanie
            {
                tasks.Add(Task.Run(() => Process(names, surname, age, rand_num, city, state))); //nasz sql += przy synchronicznej
            }
            await Task.WhenAll(tasks);//oczekiwanie na wykonanie zadań i wysłanie ich do bazy
            timerloop3.Stop();
            Console.WriteLine($"Pętle skąldająca się z {number} zapytań utworzyłem asynchronicznie i wysłałem do bazy danyche   {timerloop3.ElapsedMilliseconds / 1000}s.");
        }
        static async Task Process(DataTable names, DataTable surname, DataTable age, DataTable rand_num, DataTable city, DataTable state) //pojedyńcze zadaanie wykonywane w ramch listy tasków
        {
            string sql = "";
            var db = new DB();

            var rand = new Random();
            var name = names.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
            var sur = surname.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
            var age1 = age.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
            var rand_nums = rand_num.AsEnumerable().Select(x => x.ItemArray[1]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
            var city1 = city.AsEnumerable().Select(x => x.ItemArray[0]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();
            var state1 = state.AsEnumerable().Select(x => x.ItemArray[0]).OrderBy(r => rand.Next()).Take(1).FirstOrDefault();

            sql = $"INSERT INTO `pawel`.`asynchroniczny`(`name`,`surname`,`wiek`,`miasto`,`wojewodztwo`,`random_num`) VALUES ('{name}', '{sur}', '{age1}', '{city1}', '{state1}','{rand_nums}');"; //dodajemy do zmiennej sql kazdorazowo nowa linijke tekstu

            db.ExecuteMysqlQuerry(sql);

        }
    }
}