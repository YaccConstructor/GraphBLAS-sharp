#r "nuget: FSharp.Data"

open System
open System.IO
open System.Net
open System.IO.Compression
open FSharp.Data
open FSharp.Data.CsvExtensions

let downloadGraphs graphProvider url (outputFile: string) =
    printfn "%s %s %s" graphProvider url (Path.GetFileName outputFile)
    use client = new WebClient()
    match graphProvider with
    | "networkrepo" ->
        let archive = Path.ChangeExtension(outputFile, ".zip")
        client.DownloadFile(Uri url, archive)
        ZipFile
            .OpenRead(archive)
            .GetEntry(Path.GetFileName outputFile)
            .ExtractToFile(outputFile)
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
    let datasetOutputPath = Path.Join [| datasetRootPath; row?OutputFile |]
    downloadGraphs row?GraphProvider row?Url datasetOutputPath)

