
open GraphBLAS.FSharp.Backend
open Brahma.FSharp.OpenCL
open System.IO
// open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Common

// open GraphBLAS.FSharp

// open Brahma.FSharp.OpenCL
// open OpenCL.Net
// open GraphBLAS.FSharp.Predefined
// open Microsoft.FSharp.Quotations

// open System.IO
// open Brahma.FSharp.OpenCL
// open Brahma.FSharp.OpenCL.Translator
// open Brahma.FSharp.OpenCL.Printer.AST
// open FSharp.Quotations

type System.Random with
    /// Generates an infinite sequence of random numbers within the given range.
        member this.GetValues(minValue, maxValue) =
            Seq.initInfinite (fun _ -> this.Next(minValue, maxValue))

[<EntryPoint>]
let main argv =
    let clContext = ClContext()
    let processor = clContext.CommandQueue

    let n = 100
    let workGroupSize = 128
    let path = "test.mtx"

    let r = System.Random()
    let nums = r.GetValues(1, 20) |> Seq.take 4 |> Seq.toArray

    // let fmtx = toCSR clContext workGroupSize processor m
    // let smtx = toCSR clContext workGroupSize processor m

    // let n1 = 234305
    // let n2 = 325537
    // let n3 = 313491
    // let n4 = 122103
    let n1 = nums.[0]
    let n2 = nums.[1]
    let n3 = nums.[2]
    let n4 = nums.[3]
    printfn "n1 = %A" n1
    printfn "n2 = %A" n2
    printfn "n3 = %A" n3
    printfn "n4 = %A" n4
    let n = n1 + n2 + n3 + n4
    printfn "n = %A" n

    let p1 =
        [| 0..n1-1 |]
        |> Array.map (fun i -> i / n1)
    let p2 =
        [| 0..n2-1 |]
        |> Array.map (fun i -> i / n2 + 1)
    let p3 =
        [| 0..n3-1 |]
        |> Array.map (fun i -> i / n3 + 2)
    let p4 =
        [| 0..n4-1 |]
        |> Array.map (fun i -> i / n4 + 3)
    let prop =
        Array.append (Array.append (Array.append p1 p2) p3) p4
        |> Array.map uint64

    let keys = clContext.CreateClArray(
        prop
        // [|0..n|]
        // |> Array.map (fun i -> uint64 (n - i))
    )

    // let numOfSectors = (r.GetValues(1, n) |> Seq.take 1 |> Seq.toArray).[0]
    let numOfSectors = 5
    let mutable nums = [||]
    if numOfSectors <> 0 then
        nums <- r.GetValues(1, n / (numOfSectors - 1)) |> Seq.take (numOfSectors - 1) |> Seq.toArray

    // let m1 = int (float n * 0.37)
    // let m2 = int (float n * 0.24)
    // let m1 = nums.[0]
    // let m2 = nums.[1]
    // let m3 = prop.Length - m1 - m2
    // let m = m1 + m2 + m3

    // let c1 =
    //     [|0..m1-1|]
    //     |> Array.map (fun i ->  i / (m1 - 1))
    // let c2 =
    //     [|0..m2-1|]
    //     |> Array.map (fun i ->  i / (m2 - 1))
    // let c3 =
    //     [|0..m3-1|]
    //     |> Array.map (fun i ->  i / (m3 - 1))
    // let con = Array.append (Array.append c1 c2) c3

    let mutable sum = 0
    let mutable con = [||]
    for i in 0 .. numOfSectors - 2 do
        con <-
            [|0..nums.[i]-1|]
            |> Array.map (fun k -> if k = nums.[i] - 1 then 1 else 0)
            |> Array.append con
        sum <- sum + nums.[i]
    con <-
        [|0..prop.Length-sum-1|]
        |> Array.map (fun k -> if k = prop.Length-sum - 1 then 1 else 0)
        |> Array.append con

    let sectorTails = clContext.CreateClArray(
        con
        // [|0..prop.Length-1|]
        // |> Array.map (fun i ->  i / (prop.Length - 1))
    )

    let pun = RadixSort.createHistograms clContext workGroupSize

    let sectorIndices, sectorPointers, histograms00, histograms01, histograms10, histograms11 = pun processor keys sectorTails 31

    printfn "========================================"
    // histograms
    // |> List.iter (fun hist ->
    //     let arr = Array.zeroCreate hist.Length
    //     printfn "Bef"
    //     let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(hist, arr, ch))
    //     printfn "Aft"
    //     printfn "%A" arr
    // )
    let arr = Array.zeroCreate histograms00.Length
    // printfn "%A" histograms00.Length
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms00, arr, ch))
    printfn "%A" arr
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms01, arr, ch))
    printfn "%A" arr
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms10, arr, ch))
    printfn "%A" arr
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms11, arr, ch))
    printfn "%A" arr


    // let gg = COOMatrix.eWiseAdd clContext <@ (+) @> workGroupSize processor m m

    // let frPCPU = Array.zeroCreate gg.Rows.Length
    // let fcCPU = Array.zeroCreate gg.Columns.Length
    // let fvCPU = Array.zeroCreate gg.Values.Length

    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gg.Rows, frPCPU, ch))
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gg.Columns, fcCPU, ch))
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gg.Values, fvCPU, ch))

    // printfn "%A" gg.RowCount
    // printfn "%A" gg.ColumnCount
    // printfn "%A" frPCPU
    // printfn "%A" fcCPU
    // printfn "%A" fvCPU


    // let spgemm = CSRMatrix.spgemm <@ (*) @> <@ (+) @> 0.0 clContext workGroupSize processor
    // // let rmtx = CSRMatrix.spgemm <@ (*) @> <@ (+) @> 0.0 clContext workGroupSize processor fmtx smtx
    // for i in 0 .. 1000 do
    //     let rmtx = spgemm fmtx smtx
    //     ()

    0
