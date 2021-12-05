
open GraphBLAS.FSharp.Backend
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

open GraphBLAS.FSharp

open Brahma.FSharp.OpenCL
open OpenCL.Net
open System.IO
open GraphBLAS.FSharp.Predefined
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common.PrefixSum

open System.IO
open Brahma.FSharp.OpenCL
open Brahma.FSharp.OpenCL.Translator
open Brahma.FSharp.OpenCL.Printer.AST
open FSharp.Quotations

[<EntryPoint>]
let main argv =
    let clContext = ClContext()
    let processor = clContext.CommandQueue

    let n = 100
    let workGroupSize = 128

    let plus = <@ (+) @>

    let plusAdvanced =
        <@
            fun ((x1, x2): struct('a * int))
                ((y1, y2): struct('a * int)) ->

                if y2 = 1 then
                    struct(y1, 1)
                else
                    let buff = (%plus) x1 y1
                    struct(buff, x2)
        @>

    let frP =
        clContext.CreateClArray(
            [|0;0;0;3;3;3;6;6;6|],
            hostAccessMode = HostAccessMode.NotAccessible
        )
    let fc =
        clContext.CreateClArray(
            [|1;5;7;1;2;6|],
            hostAccessMode = HostAccessMode.NotAccessible
        )
    let fv =
        clContext.CreateClArray(
            Array.init fc.Length (fun i -> i + 1),
            hostAccessMode = HostAccessMode.NotAccessible
        )
    let fmtx = {
        RowCount = frP.Length - 1
        ColumnCount = frP.Length - 1
        RowPointers = frP
        Columns = fc
        Values = fv
    }

    let srP =
        clContext.CreateClArray(
            [|0;0;3;5;9;9;10;13;15|],
            hostAccessMode = HostAccessMode.NotAccessible
        )
    let sc =
        clContext.CreateClArray(
            [|2;4;5;1;3;2;3;5;7;1;3;4;7;0;1|],
            hostAccessMode = HostAccessMode.NotAccessible
        )
    let sv =
        clContext.CreateClArray(
            Array.init sc.Length (fun i -> i + 1),
            hostAccessMode = HostAccessMode.NotAccessible
        )
    let smtx = {
        RowCount = srP.Length - 1
        ColumnCount = srP.Length - 1
        RowPointers = srP
        Columns = sc
        Values = sv
    }

    let heads =
        clContext.CreateClArray(
            [| 1;1;0;1;0;0;1 |],
            hostAccessMode = HostAccessMode.NotAccessible
        )

    // let fmtxp = PrepareMatrix.run clContext workGroupSize processor fmtx smtx
    // let rmtx = Setup.run clContext workGroupSize processor fmtxp smtx
    // let rmtx = Expansion.run clContext workGroupSize processor fmtxp smtx rmtx <@ (*) @>
    // let rmtx = Sorting.run clContext workGroupSize processor rmtx

    let rmtx = SpGEMMSimple.run clContext workGroupSize processor fmtx smtx <@ (*) @> <@ (+) @> 0

    let frPCPU = Array.zeroCreate rmtx.RowPointers.Length
    let fcCPU = Array.zeroCreate rmtx.Columns.Length
    let fvCPU = Array.zeroCreate rmtx.Values.Length

    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(rmtx.RowPointers, frPCPU, ch))
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(rmtx.Columns, fcCPU, ch))
    let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(rmtx.Values, fvCPU, ch))

    printfn "%A" frPCPU
    printfn "%A" fcCPU
    printfn "%A" fvCPU





    // let mtx2 = Converter.toCOO clContext processor workGroupSize mtx
    // let res = Array.zeroCreate mtx2.Rows.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(mtx2.Rows, res, ch))


    //let totalSum = clContext.CreateClArray(1)



    // let n = 100
    // let keys =
    //     clContext.CreateClArray(
    //         Array.init n (fun i -> uint64 (n - i)),
    //         hostAccessMode = HostAccessMode.NotAccessible
    //     )
    // let values =
    //     clContext.CreateClArray(
    //         Array.init n (fun i -> i),
    //         hostAccessMode = HostAccessMode.NotAccessible
    //     )
    // RadixSort.sortByKeyInPlace4 clContext workGroupSize processor keys values
    // let res1 = Array.zeroCreate keys.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(keys, res1, ch))
    // let res2 = Array.zeroCreate values.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(values, res2, ch))
    // printfn "%A" res1
    // printfn "%A" res2



    // let init =
    //     <@
    //         fun (range: Range1D)
    //             (buffer: ClArray<_>) ->

    //             let i = range.GlobalID0
    //             if i < n then
    //                 buffer.[i] <- i
    //     @>
    // let kernel = clContext.CreateClKernel(init)
    // let ndRange = Range1D.CreateValid(n, workGroupSize)
    // processor.Post(
    //     Msg.MsgSetArguments
    //         (fun () ->
    //             kernel.ArgumentsSetter
    //                 ndRange
    //                 xs)
    // )
    // processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    // let init =
    //     <@
    //         fun (range: Range1D)
    //             (buffer: ClArray<_>) ->

    //             let i = range.GlobalID0
    //             if i < n then buffer.[i] <- i
    //     @>
    // let ndRange = Range1D.CreateValid(n, workGroupSize)
    // // let kernel = clContext.CreateClKernel(init)
    // // let ndRange = Range1D.CreateValid(n, workGroupSize)
    // // processor.Post(
    // //     Msg.MsgSetArguments
    // //         (fun () ->
    // //             kernel.ArgumentsSetter
    // //                 ndRange
    // //                 ys)
    // // )
    // // processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    //RadixSort.sortByKeyInPlace clContext workGroupSize processor xs ys 2

    // let length = xs.Length
    // let klukva =
    //     <@
    //         fun (range: Range1D)
    //             (buffer: ClArray<_>) ->

    //             let i = range.GlobalID0
    //             if i < length then
    //                 atomic (+) buffer.[0] buffer.[i] |> ignore
    //                 //buffer.[0] <- a
    //     @>
    // let kernel = clContext.CreateClKernel(klukva)
    // let ndRange = Range1D.CreateValid(xs.Length, workGroupSize)
    // processor.Post(
    //     Msg.MsgSetArguments
    //         (fun () ->
    //             kernel.ArgumentsSetter
    //                 ndRange
    //                 xs)
    // )
    // processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    // let _ = PrefixSum.runIncludeInplace clContext workGroupSize processor xs totalSum 0 <@ (+) @> 0

    // let update =
    //     <@
    //         fun (range: Range1D)
    //             (buffer: ClArray<_>) ->

    //             let i = range.GlobalID0
    //             if i < n then
    //                 let struct(a, b) = buffer.[i]
    //                 let klukva = if a % 3 = 0 then 1 else 0
    //                 buffer.[i] <- (%opAdd2) buffer.[i] struct(a + 1, klukva)
    //     @>
    // let kernel = clContext.CreateClKernel(update)
    // let ndRange = Range1D.CreateValid(n, workGroupSize)
    // processor.Post(
    //     Msg.MsgSetArguments
    //         (fun () ->
    //             kernel.ArgumentsSetter
    //                 ndRange
    //                 xs)
    // )
    // processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    // let vertices =
    //     clContext.CreateClArray(
    //         (n - 1) / workGroupSize + 1,
    //         hostAccessMode = HostAccessMode.NotAccessible
    //     )

    // let _ = ClArray.prefixSumExcludeInplace clContext workGroupSize processor xs totalSum

    // let res = Array.zeroCreate xs.Length
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(xs, res, ch))
    // processor.Post(Msg.CreateFreeMsg<_>(xs))
    // processor.Post(Msg.CreateFreeMsg<_>(ys))

    // printfn "%A" res

    0
