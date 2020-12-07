#r "nuget: FSharp.Data"

open System
open System.IO
open System.Net
open System.IO.Compression
open FSharp.Data
open FSharp.Data.CsvExtensions

let downloadAndUnzip graphName archiveType url outputDir =
    use client = new WebClient()
    match archiveType with
    | "zip" ->
        let archive = Path.Combine [| outputDir; Path.ChangeExtension(graphName, ".zip") |]
        client.AsyncDownloadFile(Uri url, archive) |> Async.RunSynchronously
        ZipFile.ExtractToDirectory(archive, outputDir)
    | _ -> ()

(*
    Get all "graphs.csv" flies
    -> load all rows from all csv files
    -> download and unzip graph archives
*)
seq {
    for dir in fsi.CommandLineArgs.[1..] do
        yield! Directory.EnumerateFiles(dir, "graphs.csv", SearchOption.AllDirectories)
}
|> Seq.map Path.GetFullPath
|> Seq.collect (fun pathToCsv ->
    CsvFile.Load(pathToCsv, separators=",", hasHeaders=true).Rows |> Seq.allPairs <| Seq.singleton pathToCsv)
|> Seq.iter (fun (row, pathToCsv) ->
    let datasetRootPath = Path.GetDirectoryName pathToCsv
    downloadAndUnzip row?GraphName row?ArchiveType row?Url datasetRootPath)

