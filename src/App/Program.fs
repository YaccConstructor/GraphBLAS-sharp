
open GraphBLAS.FSharp.Backend
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

open GraphBLAS.FSharp

open Brahma.FSharp.OpenCL
open OpenCL.Net
open System.IO
open GraphBLAS.FSharp.Predefined

[<EntryPoint>]
let main argv =
    let clContext = ClContext()
    let processor = clContext.CommandQueue

    let n = 1000000
    let workGroupSize = 128

    let opAdd =
        <@
            fun ((x1,x2): struct(int*int))
                ((y1,y2): struct(int*int)) ->

                if y2 = 1 then struct(y1,1) else struct(x1+y1,x2)
        @>


    let xs =
        clContext.CreateClArray(
            n,
            hostAccessMode = HostAccessMode.NotAccessible
        )

    let totalSum = clContext.CreateClArray(1)

    let init =
        <@
            fun (range: Range1D)
                (buffer: ClArray<_>) ->

                let i = range.GlobalID0
                if i < n then
                    let klukva = if i % 7 = 0 then 1 else 0
                    buffer.[i] <- struct(i / 7, klukva)
        @>
    let kernel = clContext.CreateClKernel(init)
    let ndRange = Range1D.CreateValid(n, workGroupSize)
    processor.Post(
        Msg.MsgSetArguments
            (fun () ->
                kernel.ArgumentsSetter
                    ndRange
                    xs)
    )
    processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    //let _ = PrefixSumGeneric.prefixSumExcludeInplace clContext workGroupSize processor xs totalSum opAdd struct(0, 0)

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


    let res = Array.zeroCreate n
    let _ =
        processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(xs, res, ch))
    processor.Post(Msg.CreateFreeMsg<_>(xs))

    processor.PostAndReply <| MsgNotifyMe

    printfn "%A" res

    0
