#r "nuget: FSharp.Data"
#r "nuget: Luna.ConsoleProgressBar"

open System
open System.IO
open System.Net
open System.IO.Compression
open FSharp.Data
open FSharp.Data.CsvExtensions
open Luna.ConsoleProgressBar

let downloadGraphs graphName archiveType url (outputDir: string) =
    use client = new WebClient()
    // use bar = new ConsoleProgressBar()
    // client.DownloadProgressChanged.Add (fun e ->
    //     bar.Report (float e.ProgressPercentage / 100.)
    // )
    match archiveType with
    | "zip" ->
        let archive = Path.Combine [| outputDir; Path.ChangeExtension(graphName, ".zip") |]
        client.AsyncDownloadFile(Uri url, archive) |> Async.RunSynchronously
        ZipFile.ExtractToDirectory(archive, outputDir)
    | _ -> ()

seq {
    for dir in fsi.CommandLineArgs.[1..] do
        yield! Directory.EnumerateFiles(dir, "graphs.csv", SearchOption.AllDirectories)
}
|> Seq.map Path.GetFullPath
|> Seq.collect (fun pathToCsv ->
    CsvFile.Load(pathToCsv, separators=",", hasHeaders=true).Rows |> Seq.allPairs <| Seq.singleton pathToCsv)
|> Seq.iter (fun (row, pathToCsv) ->
    let datasetRootPath = Path.GetDirectoryName pathToCsv
    downloadGraphs row?GraphName row?ArchiveType row?Url datasetRootPath)

