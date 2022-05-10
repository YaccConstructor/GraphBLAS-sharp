
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

    let numOfSectors = 10
    let n = 1000 * numOfSectors
    let workGroupSize = 128
    let path = "test.mtx"

    let r = System.Random()
    let nums = r.GetValues(1, 20) |> Seq.take 4 |> Seq.toArray

    let maxValue = 70

    let rsort = RadixSort.run clContext workGroupSize

    let mutable res = true

    for k in 0 .. 10 do
        printfn "%A ..." k
        let prop = r.GetValues(0, maxValue) |> Seq.take n |> Seq.toArray |> Array.map uint64
        // let prop = [|14UL; 69UL; 9UL; 40UL; 38UL; 61UL; 32UL; 31UL; 31UL; 68UL; 45UL; 1UL; 20UL; 0UL; 39UL; 24UL; 57UL; 27UL; 47UL; 24UL; 10UL; 38UL; 46UL; 57UL; 64UL; 53UL; 12UL; 16UL; 40UL; 43UL; 34UL; 1UL; 54UL; 27UL; 18UL; 22UL; 39UL; 59UL; 28UL|]
        // let prop = [|9UL; 14UL; 31UL; 40UL; 38UL; 32UL; 61UL; 69UL; 31UL; 68UL; 22UL; 12UL; 1UL; 1UL; 0UL; 39UL; 10UL; 28UL; 20UL; 27UL; 24UL; 16UL; 27UL; 18UL; 57UL; 39UL; 45UL; 47UL; 40UL; 43UL; 34UL; 38UL; 46UL; 24UL; 57UL; 53UL; 54UL; 59UL; 64UL|]
        // let prop =
        //     [|17UL; 60UL; 19UL; 19UL; 55UL; 33UL; 55UL; 37UL; 64UL; 63UL; 34UL; 43UL; 36UL; 22UL; 65UL; 22UL; 61UL; 41UL; 68UL; 45UL; 39UL; 17UL; 0UL; 45UL; 26UL; 39UL; 12UL; 42UL; 7UL; 12UL; 45UL; 30UL; 32UL; 38UL; 12UL; 65UL; 1UL; 13UL; 6UL|]
        //     |> Array.map (fun n -> uint64((int n)%4))

        printfn "%A" prop

        let n = prop.Length
        let keys = clContext.CreateClArray(
            prop
            // [|0..n|]
            // |> Array.map (fun i -> uint64 (n - i))
        )
        let mutable nums = [||]//[| for i in 0 .. numOfSectors - 2 do n / numOfSectors |]
        if numOfSectors > 1 then
            nums <- r.GetValues(1, n / (numOfSectors - 1)) |> Seq.take (numOfSectors - 1) |> Seq.toArray

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

        let keys = rsort processor keys sectorTails maxValue

        // printfn "========================================"

        let arr = Array.zeroCreate keys.Length
        let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(keys, arr, ch))
        printfn "%A" arr

        let mutable prop2 = [||]
        let len = n / numOfSectors
        let mutable acc = 0
        for j in 0 .. numOfSectors - 2 do
            let prop3 = Array.sort prop.[acc .. acc + nums.[j] - 1]
            acc <- acc + nums.[j]
            prop2 <- Array.append prop2 prop3
        let prop3 = Array.sort prop.[acc .. prop.Length - 1]
        prop2 <- Array.append prop2 prop3
        printfn "%A" prop2

        let resCurr =
            (arr, prop2)
            ||> Array.zip
            |> Array.map (fun (a, b) -> a = b)
            |> Array.fold (&&) true
        printfn "%A" resCurr
        res <- res && resCurr

    printfn "Итого %A" res

    // let prop = r.GetValues(0, maxValue) |> Seq.take n |> Seq.toArray |> Array.map uint64
    // let prop = [|41UL; 7UL; 2UL; 40UL; 41UL; 48UL; 35UL; 49UL; 24UL; 18UL; 59UL; 52UL; 24UL; 10UL; 18UL; 23UL; 53UL; 45UL; 20UL; 4UL; 43UL; 68UL; 4UL; 57UL|]
    // let prop = [|10UL; 17UL; 24UL; 18UL; 24UL; 23UL; 59UL; 52UL|] // numOfSectors = 1, cycle
    // let prop = [|2UL; 0UL; 2UL; 0UL; 2UL; 1UL; 2UL; 1UL|] // numOfSectors = 1
    // let prop = [|0UL; 17UL; 24UL; 18UL; 24UL; 23UL; 70UL; 70UL|] // numOfSectors = 1
    // let prop = [|0UL; 1UL; 3UL; 0UL; 3UL; 3UL; 3UL; 0UL|] // numOfSectors = 1


//[|17UL; 39UL; 35UL; 61UL; 17UL; 15UL; 31UL; 6UL; 66UL; 61UL; 49UL; 61UL; 0UL|]

    // printfn "%A" prop
    // let n = prop.Length
    // let keys = clContext.CreateClArray(
    //     prop
    //     // [|0..n|]
    //     // |> Array.map (fun i -> uint64 (n - i))
    // )

    // let mutable nums = [||]
    // if numOfSectors > 1 then
    //     nums <- r.GetValues(1, n / (numOfSectors - 1)) |> Seq.take (numOfSectors - 1) |> Seq.toArray
    // nums <- [| for i in 0 .. numOfSectors - 2 do n / numOfSectors |]

    // let mutable sum = 0
    // let mutable con = [||]
    // for i in 0 .. numOfSectors - 2 do
    //     con <-
    //         [|0..nums.[i]-1|]
    //         |> Array.map (fun k -> if k = nums.[i] - 1 then 1 else 0)
    //         |> Array.append con
    //     sum <- sum + nums.[i]
    // con <-
    //     [|0..prop.Length-sum-1|]
    //     |> Array.map (fun k -> if k = prop.Length-sum - 1 then 1 else 0)
    //     |> Array.append con

    // con <- [|1; 0; 0; 0; 0; 1; 0; 1|]
    // con <- [|1; 1; 1; 1; 1; 1; 0; 1; 1; 1; 1; 1; 1|]

    // let sectorTails = clContext.CreateClArray(
    //     con
    //     // [|0..prop.Length-1|]
    //     // |> Array.map (fun i ->  i / (prop.Length - 1))
    // )

    // let keys = rsort processor keys sectorTails maxValue

    // let arr = Array.zeroCreate sectorTails.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(sectorTails, arr, ch))
    // printfn "sectorTails \n%A" arr

    // let pun = RadixSort.createHistograms clContext workGroupSize
    // let sectorIndices, sectorPointers, histograms = pun processor keys sectorTails 31

    // let qun = RadixSort.permute clContext workGroupSize
    // let sectorIndices, sectorPointers, histograms, garbage, keys = qun processor keys sectorTails sectorIndices sectorPointers histograms 31

    // let arr = Array.zeroCreate garbage.[0].Length
    // garbage
    // |> List.iter (fun gar ->
    //     let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(gar, arr, ch))
    //     printfn "%A" arr
    //     // printfn "%A" (Array.fold (fun s a -> s + a) 0 arr)
    // )

    // let gun = RadixSort.computeGarbage clContext workGroupSize
    // let keys, garbageOffsets, bucketTails = gun processor keys sectorTails sectorIndices sectorPointers histograms garbage 31

    // let arr = Array.zeroCreate garbageOffsets.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(garbageOffsets, arr, ch))
    // printfn "garbageOffsets \n%A" arr

    // let arr = Array.zeroCreate bucketTails.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(bucketTails, arr, ch))
    // printfn "bucketTails \n%A" arr

    // let kun = RadixSort.repair clContext workGroupSize
    // let keysAncilla = kun processor keys sectorTails sectorIndices sectorPointers histograms garbage garbageOffsets bucketTails 31

    // let arr = Array.zeroCreate histograms.[0].Length
    // histograms
    // |> List.iter (fun hist ->
    //     let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(hist, arr, ch))
    //     printfn "%A" arr
    //     // printfn "%A" (Array.fold (fun s a -> s + a) 0 arr)
    // )

    // let arr = Array.zeroCreate keysAncilla.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(keysAncilla, arr, ch))
    // printfn "keysAncilla \n%A" arr

    // let arr = Array.zeroCreate keys.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(keys, arr, ch))
    // printfn "%A" arr

    // let prop2 = Array.sort prop
    // let mutable prop2 = [||]
    // let len = n / numOfSectors
    // for j in 0 .. numOfSectors - 1 do
    //     let prop3 = Array.sort prop.[j * len .. (j + 1) * len - 1]
    //     prop2 <- Array.append prop2 prop3
    // printfn "%A" prop2

    // (arr, prop2)
    // ||> Array.zip
    // |> Array.map (fun (a, b) -> a = b)
    // |> Array.fold (&&) true
    // |> printfn "%A"

    // let arr = Array.zeroCreate histograms.[0].Length
    // histograms
    // |> List.iter (fun hist ->
    //     let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(hist, arr, ch))
    //     printfn "%A" arr
    //     printfn "%A" (Array.fold (fun s a -> s + a) 0 arr)
    // )
    // let arr = Array.zeroCreate histograms00.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms00, arr, ch))
    // printfn "%A" arr
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms01, arr, ch))
    // printfn "%A" arr
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms10, arr, ch))
    // printfn "%A" arr
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(histograms11, arr, ch))
    // printfn "%A" arr


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
