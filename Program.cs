using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Concurrent;
using System.Diagnostics;
using static System.Linq.ParallelEnumerable;


//Папка с данными ищется автоматически (можно передать путь первым аргументом)
string dataRoot = args.Length > 0 ? args[0] : FindDataRoot();
string[] folders =
{
    Path.Combine(dataRoot, "new_data_10"),
    Path.Combine(dataRoot, "new_data_50"),
    Path.Combine(dataRoot, "new_data_100"),
    Path.Combine(dataRoot, "new_data_500")
};
int [] nums = [2, 4, 8, 12, 16, 20];


//чтение файлов
foreach (var folder in folders)
{
    Console.WriteLine();
    Console.WriteLine("ПОСЛЕДОВАТЕЛЬНЫЙ МЕТОД");
    Console.WriteLine();


    var files = Directory.EnumerateFiles(folder, "*.txt").ToList();
    Console.WriteLine();
    Console.WriteLine($"Папка: {folder} (файлов: {files.Count})");
    Console.WriteLine();


    var sw = Stopwatch.StartNew();
    var counts = LineCount(files);
    sw.Stop();
    Console.WriteLine($"Последовательно: {sw.ElapsedMilliseconds} мс");


    //сортировка вывода слов и их частоты (последовательная обработка)
    var sorted = counts.OrderByDescending(pair => pair.Value).ToList();
    Console.WriteLine("Самые частые");
    foreach (var pairs in sorted.Take(10))
    {
        Console.WriteLine($"{pairs.Key} -> {pairs.Value}");
    }
    Console.WriteLine();
    Console.WriteLine("Самые редкие");
    foreach (var pairs in sorted.TakeLast(10))
    {
        Console.WriteLine($"{pairs.Key} -> {pairs.Value}");
    }


    //параллельная реализация
    Console.WriteLine();
    Console.WriteLine("ПРАЛЛЕЛЬНЫЙ МЕТОД");
    Console.WriteLine();


foreach (var item in nums)
{
    var swParallel = Stopwatch.StartNew();
    var parallel_counts = CountParallelMapReduce(files, item);
    swParallel.Stop();
    Console.WriteLine($"Потоков: {item}, время: {swParallel.ElapsedMilliseconds} мс");
}
}


//Фцнкция для последовательной обработки
static Dictionary<string, int> LineCount(List<string> files)
{
    var counts_words = new Dictionary<string, int>();
    foreach (var path in files)
    {
        foreach (var line in File.ReadLines(path))
        {
            foreach (var word in Pull(line))
            {
                if (counts_words.ContainsKey(word))
                {
                    counts_words[word] = counts_words[word] + 1;
                }
                else
                {
                    counts_words[word] = 1;
                }
            }
        }
    }
    return counts_words;
}


//Функция для параллельной обработки MapReduce
static ConcurrentDictionary<string,int> CountParallelMapReduce(List<string> files, int degreeOfParallelism)
{
    var global_counts_words = new ConcurrentDictionary<string, int>();


   
    Parallel.ForEach(
        files,
        new ParallelOptions {MaxDegreeOfParallelism = degreeOfParallelism},
        () => new Dictionary<string,int>(),
        (path, state, localCounts) =>
        {
            foreach (var line in File.ReadLines(path))
            {
            foreach (var word in Pull(line))
                {
                if (localCounts.ContainsKey(word))
                {
                    localCounts[word] = localCounts[word] + 1;
                }
                else
                {
                    localCounts[word] = 1;
                }
                }
            }
            return localCounts;
        },
        (localCounts) =>
        {
            foreach (var pair in localCounts)
            {
                global_counts_words.AddOrUpdate(pair.Key, pair.Value, (key, oldValue) => oldValue + pair.Value);
            }
        });
    return global_counts_words;
}


//Функция для поиска папки с данными: поднимается вверх от папки с exe,
//пока не найдёт папку, в которой лежит new_data_10
static string FindDataRoot()
{
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    while (dir != null)
    {
        if (Directory.Exists(Path.Combine(dir.FullName, "new_data_10")))
        {
            return dir.FullName;
        }
        dir = dir.Parent;
    }
    return Directory.GetCurrentDirectory();
}


//Функция для очистки текста
static IEnumerable<string> Pull(string line)
{
    var clean = new string(line.Select(ch => char.IsLetter(ch) ? char.ToLower(ch): ' ').ToArray());
    return clean.Split(' ', StringSplitOptions.RemoveEmptyEntries);
}
