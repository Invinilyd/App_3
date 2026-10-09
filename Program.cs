using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;


//Названия файлов нужно заменить при запуске кода на другом пк!!!
//(сейчас папка с данными ищется автоматически рядом с проектом, путь можно передать первым аргументом)
string data_root = args.Length > 0 ? args[0] : FindDataRoot();
string[] folders =
{
    Path.Combine(data_root, "new_data_10"),
    Path.Combine(data_root, "new_data_50"),
    Path.Combine(data_root, "new_data_100"),
    Path.Combine(data_root, "new_data_500")
};

//число потоков для задания 1
int[] nums = [2, 4, 8, 12, 16, 20];
//число потоков PLINQ для заданий 2 и 3
int[] nums_plinq = [2, 4, 8, 12];

//слова для поиска предложений (задание 2)
string[] search_words = ["that", "you", "romeo", "vengeance", "mirror"];
//слово для поиска контекста (задание 3)
string target_word = "romeo";

//пул потоков сразу создаёт нужное число потоков, иначе он добавляет их медленно
//и время для большого числа потоков получается завышенным
ThreadPool.SetMinThreads(nums.Max(), nums.Max());


//меню
while (true)
{
    Console.WriteLine();
    Console.WriteLine("Выберите задание:");
    Console.WriteLine("1 - Задание 1 (частота слов)");
    Console.WriteLine("2 - Задание 2 (поиск предложений со словами)");
    Console.WriteLine("3 - Задание 3 (контекст слова)");
    Console.WriteLine("0 - Выход");
    Console.Write("Ваш выбор: ");

    var choice = Console.ReadLine();
    if (choice == null || choice.Trim() == "0")
    {
        break;
    }

    switch (choice.Trim())
    {
        case "1":
            RunTask1(folders, nums);
            break;
        case "2":
            RunTask2(folders, nums_plinq, search_words);
            break;
        case "3":
            RunTask3(folders, nums_plinq, target_word);
            break;
        default:
            Console.WriteLine("Неверный ввод, введите 0, 1, 2 или 3");
            break;
    }
}


//==================== ЗАДАНИЕ 1 ====================
//Подсчёт частоты слов: последовательно и параллельно (MapReduce)

static void RunTask1(string[] folders, int[] nums)
{
    Console.WriteLine();
    Console.WriteLine("ЗАДАНИЕ 1. ЧАСТОТА СЛОВ");

    //прогрев на маленькой папке, чтобы JIT-компиляция не попала в замеры
    var warm_files = Directory.EnumerateFiles(folders[0], "*.txt").ToList();
    LineCount(warm_files);
    CountParallelMapReduce(warm_files, 2);

    foreach (var folder in folders)
    {
        var files = Directory.EnumerateFiles(folder, "*.txt").ToList();
        Console.WriteLine();
        Console.WriteLine($"Папка: {folder} (файлов: {files.Count})");

        //холостой прогон, чтобы первое чтение с диска не искажало замер
        LineCount(files);

        //последовательная обработка
        var sw = Stopwatch.StartNew();
        var counts = LineCount(files);
        sw.Stop();
        Console.WriteLine($"Последовательно: {sw.ElapsedMilliseconds} мс");

        //параллельная обработка
        Console.WriteLine("Параллельно (MapReduce):");
        foreach (var item in nums)
        {
            var sw_parallel = Stopwatch.StartNew();
            var parallel_counts = CountParallelMapReduce(files, item);
            sw_parallel.Stop();

            bool same = parallel_counts.Count == counts.Count
                && parallel_counts.Values.Sum() == counts.Values.Sum();
            Console.WriteLine($"Потоков: {item}, время: {sw_parallel.ElapsedMilliseconds} мс, совпадает с последовательным: {same}");
        }

        //ранжированная статистика (по результату последовательной версии)
        var sorted = counts.OrderByDescending(pair => pair.Value).ToList();
        Console.WriteLine();
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
    }
}


//Функция для последовательной обработки
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
static ConcurrentDictionary<string, int> CountParallelMapReduce(List<string> files, int degreeOfParallelism)
{
    var global_counts_words = new ConcurrentDictionary<string, int>();

    Parallel.ForEach(
        files,
        new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism },
        //Map: у каждого потока свой локальный словарь
        () => new Dictionary<string, int>(),
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
        //Reduce: слияние локальных словарей в общий
        (localCounts) =>
        {
            foreach (var pair in localCounts)
            {
                global_counts_words.AddOrUpdate(pair.Key, pair.Value, (key, oldValue) => oldValue + pair.Value);
            }
        });
    return global_counts_words;
}


//==================== ЗАДАНИЕ 2 ====================
//Поиск предложений с заданными словами: LINQ и PLINQ

static void RunTask2(string[] folders, int[] nums_plinq, string[] search_words)
{
    Console.WriteLine();
    Console.WriteLine("ЗАДАНИЕ 2. ПОИСК ПРЕДЛОЖЕНИЙ СО СЛОВАМИ: " + string.Join(", ", search_words));

    //прогрев на маленькой папке
    var warm_files = Directory.EnumerateFiles(folders[0], "*.txt").ToList();
    FindSentencesLinq(warm_files, search_words);
    FindSentencesPlinq(warm_files, search_words, 2);

    foreach (var folder in folders)
    {
        var files = Directory.EnumerateFiles(folder, "*.txt").ToList();
        Console.WriteLine();
        Console.WriteLine($"Папка: {folder} (файлов: {files.Count})");

        //холостой прогон
        FindSentencesLinq(files, search_words);

        //последовательная версия (LINQ)
        var sw = Stopwatch.StartNew();
        var sentences_by_word = FindSentencesLinq(files, search_words);
        sw.Stop();
        int total_linq = sentences_by_word.Values.Sum(list => list.Count);
        Console.WriteLine($"LINQ: {sw.ElapsedMilliseconds} мс (предложений: {total_linq})");

        //параллельная версия (PLINQ)
        foreach (var item in nums_plinq)
        {
            var sw_parallel = Stopwatch.StartNew();
            var parallel_sentences = FindSentencesPlinq(files, search_words, item);
            sw_parallel.Stop();

            int total_plinq = parallel_sentences.Values.Sum(list => list.Count);
            Console.WriteLine($"PLINQ, потоков: {item}, время: {sw_parallel.ElapsedMilliseconds} мс, совпадает с LINQ: {total_plinq == total_linq}");
        }

        //вывод результата: число предложений и до 3 примеров на слово
        Console.WriteLine();
        foreach (var word in search_words)
        {
            if (!sentences_by_word.ContainsKey(word))
            {
                Console.WriteLine($"{word}: не найдено");
                continue;
            }

            var found = sentences_by_word[word];
            Console.WriteLine($"{word}: найдено предложений: {found.Count}");
            foreach (var sentence in found.Take(3))
            {
                Console.WriteLine($"   - {sentence}");
            }
        }
    }
}


//Функция для чтения предложений из файла построчно
static IEnumerable<string> ReadSentences(string path)
{
    string buffer = "";
    foreach (var line in File.ReadLines(path))
    {
        //неоконченное предложение переносится на следующую строку
        buffer = buffer.Length == 0 ? line : buffer + " " + line;

        var parts = Regex.Split(buffer, @"(?<=[.!?])\s+");
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (!string.IsNullOrWhiteSpace(parts[i]))
            {
                yield return parts[i].Trim();
            }
        }
        buffer = parts[parts.Length - 1];
    }

    //последнее предложение файла (может быть без точки)
    if (!string.IsNullOrWhiteSpace(buffer))
    {
        yield return buffer.Trim();
    }
}


//Функция для последовательного поиска предложений (LINQ)
static Dictionary<string, List<string>> FindSentencesLinq(List<string> files, string[] search_words)
{
    return files
        .SelectMany(path => ReadSentences(path))
        .SelectMany(sentence =>
        {
            var words = new HashSet<string>(Pull(sentence));
            return search_words
                .Where(word => words.Contains(word))
                .Select(word => (word, sentence));
        })
        .GroupBy(pair => pair.word)
        .ToDictionary(group => group.Key, group => group.Select(pair => pair.sentence).ToList());
}


//Функция для параллельного поиска предложений (PLINQ)
static Dictionary<string, List<string>> FindSentencesPlinq(List<string> files, string[] search_words, int degreeOfParallelism)
{
    return files
        .AsParallel()
        .WithDegreeOfParallelism(degreeOfParallelism)
        .SelectMany(path => ReadSentences(path))
        .SelectMany(sentence =>
        {
            var words = new HashSet<string>(Pull(sentence));
            return search_words
                .Where(word => words.Contains(word))
                .Select(word => (word, sentence));
        })
        .GroupBy(pair => pair.word)
        .ToDictionary(group => group.Key, group => group.Select(pair => pair.sentence).ToList());
}


//==================== ЗАДАНИЕ 3 ====================
//Вывод контекста заданного слова: LINQ и PLINQ

static void RunTask3(string[] folders, int[] nums_plinq, string target_word)
{
    Console.WriteLine();
    Console.WriteLine($"ЗАДАНИЕ 3. КОНТЕКСТ СЛОВА: {target_word}");

    //прогрев на маленькой папке
    var warm_files = Directory.EnumerateFiles(folders[0], "*.txt").ToList();
    CountContextLinq(warm_files, target_word);
    CountContextPlinq(warm_files, target_word, 2);

    foreach (var folder in folders)
    {
        var files = Directory.EnumerateFiles(folder, "*.txt").ToList();
        Console.WriteLine();
        Console.WriteLine($"Папка: {folder} (файлов: {files.Count})");

        //холостой прогон
        CountContextLinq(files, target_word);

        //последовательная версия (LINQ)
        var sw = Stopwatch.StartNew();
        var context_counts = CountContextLinq(files, target_word);
        sw.Stop();
        int total_linq = context_counts.Values.Sum();
        Console.WriteLine($"LINQ: {sw.ElapsedMilliseconds} мс (контекстных слов: {total_linq}, уникальных: {context_counts.Count})");

        //параллельная версия (PLINQ)
        foreach (var item in nums_plinq)
        {
            var sw_parallel = Stopwatch.StartNew();
            var parallel_counts = CountContextPlinq(files, target_word, item);
            sw_parallel.Stop();

            bool same = parallel_counts.Values.Sum() == total_linq
                && parallel_counts.Count == context_counts.Count;
            Console.WriteLine($"PLINQ, потоков: {item}, время: {sw_parallel.ElapsedMilliseconds} мс, совпадает с LINQ: {same}");
        }

        //вывод результата: контекстные слова по убыванию частоты
        Console.WriteLine();
        if (context_counts.Count == 0)
        {
            Console.WriteLine($"{target_word}: не найдено");
            continue;
        }

        var sorted = context_counts.OrderByDescending(pair => pair.Value).ToList();
        Console.WriteLine($"{target_word} -> самые частые контекстные слова");
        foreach (var pairs in sorted.Take(10))
        {
            Console.WriteLine($"{pairs.Key} -> {pairs.Value}");
        }
        Console.WriteLine();
        Console.WriteLine($"{target_word} -> самые редкие контекстные слова");
        foreach (var pairs in sorted.TakeLast(10))
        {
            Console.WriteLine($"{pairs.Key} -> {pairs.Value}");
        }
    }
}


//Функция для поиска соседних слов в файле (соседи ищутся в пределах одной строки)
static IEnumerable<string> ReadContextWords(string path, string target_word)
{
    foreach (var line in File.ReadLines(path))
    {
        var tokens = Pull(line).ToList();
        for (int i = 0; i < tokens.Count; i++)
        {
            if (tokens[i] != target_word)
            {
                continue;
            }

            //левый сосед
            if (i > 0)
            {
                yield return tokens[i - 1];
            }
            //правый сосед
            if (i < tokens.Count - 1)
            {
                yield return tokens[i + 1];
            }
        }
    }
}


//Функция для последовательного подсчёта контекстных слов (LINQ)
static Dictionary<string, int> CountContextLinq(List<string> files, string target_word)
{
    return files
        .SelectMany(path => ReadContextWords(path, target_word))
        .GroupBy(word => word)
        .ToDictionary(group => group.Key, group => group.Count());
}


//Функция для параллельного подсчёта контекстных слов (PLINQ)
static Dictionary<string, int> CountContextPlinq(List<string> files, string target_word, int degreeOfParallelism)
{
    return files
        .AsParallel()
        .WithDegreeOfParallelism(degreeOfParallelism)
        .SelectMany(path => ReadContextWords(path, target_word))
        .GroupBy(word => word)
        .ToDictionary(group => group.Key, group => group.Count());
}


//==================== ОБЩАЯ ФУНКЦИЯ ====================

//Функция для очистки текста
static IEnumerable<string> Pull(string line)
{
    var clean = new string(line.Select(ch => char.IsLetter(ch) ? char.ToLower(ch) : ' ').ToArray());
    return clean.Split(' ', StringSplitOptions.RemoveEmptyEntries);
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
