
open GraphBLAS.FSharp.Backend
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

open GraphBLAS.FSharp

open Brahma.FSharp.OpenCL
open OpenCL.Net
open System.IO
open GraphBLAS.FSharp.Predefined
open Microsoft.FSharp.Quotations


[<EntryPoint>]
let main argv =
    let clContext = ClContext()
    let processor = clContext.CommandQueue

    let n = 100
    let workGroupSize = 128



    // let opAdd =
    //     <@
    //         fun ((x1,x2): struct(int*int))
    //             ((y1,y2): struct(int*int)) ->

    //             if y2 = 1 then struct(y1,1) else struct(x1+y1,x2)
    //     @>


    let xs =
        clContext.CreateClArray(
            n,
            hostAccessMode = HostAccessMode.NotAccessible
        )

    let ys =
        clContext.CreateClArray(
            n,
            hostAccessMode = HostAccessMode.NotAccessible
        )

    let totalSum = clContext.CreateClArray(1)

    // let init =
    //     <@
    //         fun (range: Range1D)
    //             (buffer: ClArray<_>) ->

    //             let i = range.GlobalID0
    //             if i < n then buffer.[i] <- i
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


    // RadixSort.sortByKeyInPlace clContext workGroupSize processor xs ys 2


    // let _ = PrefixSum.runExcludeInplace clContext workGroupSize processor xs totalSum 0 <@ (+) @> 0.0

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


    // let scanGeneral beforeLocalSumClear writeData (clContext: ClContext) workGroupSize =
    //     fun (processor: MailboxProcessor<_>)
    //         (inputArray: ClArray<'a>)
    //         (inputArrayLength: int)
    //         (vertices: ClArray<'a>)
    //         (verticesLength: int)
    //         (totalSum: ClArray<'a>)
    //         idxToWrite
    //         (opAdd: Expr<'a -> 'a -> 'a>)
    //         (zero: 'a) ->

    //         let scan =
    //             <@ fun
    //                 (ndRange: Range1D)
    //                 inputArrayLength
    //                 verticesLength
    //                 (resultBuffer: ClArray<'a>)
    //                 (verticesBuffer: ClArray<'a>)
    //                 (totalSumBuffer: ClArray<'a>) ->

    //                 let resultLocalBuffer = localArray<'a> workGroupSize
    //                 let i = ndRange.GlobalID0
    //                 let localID = ndRange.LocalID0

    //                 if i < inputArrayLength then
    //                     resultLocalBuffer.[localID] <- resultBuffer.[i]
    //                 else
    //                     resultLocalBuffer.[localID] <- zero

    //                 let mutable step = 2

    //                 while step <= workGroupSize do
    //                     barrier ()

    //                     if localID < workGroupSize / step then
    //                         let i = step * (localID + 1) - 1

    //                         resultLocalBuffer.[i] <- (%opAdd) resultLocalBuffer.[i - (step >>> 1)] resultLocalBuffer.[i]

    //                     step <- step <<< 1

    //                 barrier ()

    //                 if localID = workGroupSize - 1 then
    //                     if verticesLength <= 1 && localID = i then
    //                         totalSumBuffer.[idxToWrite] <- resultLocalBuffer.[localID]

    //                     verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID]
    //                     (%beforeLocalSumClear) resultBuffer resultLocalBuffer.[localID] inputArrayLength i
    //                     resultLocalBuffer.[localID] <- zero

    //                 step <- workGroupSize

    //                 while step > 1 do
    //                     barrier ()

    //                     if localID < workGroupSize / step then
    //                         let i = step * (localID + 1) - 1
    //                         let j = i - (step >>> 1)

    //                         let tmp = resultLocalBuffer.[i]
    //                         resultLocalBuffer.[i] <- (%opAdd) resultLocalBuffer.[i] resultLocalBuffer.[j]
    //                         resultLocalBuffer.[j] <- tmp

    //                     step <- step >>> 1

    //                 barrier ()

    //                 (%writeData) resultBuffer resultLocalBuffer inputArrayLength workGroupSize i localID
    //             @>

    //         let kernel = clContext.CreateClKernel scan
    //         let ndRange = Range1D.CreateValid(inputArrayLength, workGroupSize)
    //         processor.Post(
    //             Msg.MsgSetArguments
    //                 (fun () -> kernel.ArgumentsSetter ndRange inputArrayLength verticesLength inputArray vertices totalSum)
    //         )
    //         processor.Post(Msg.CreateRunMsg<_, _> kernel)

    // let vertices =
    //     clContext.CreateClArray(
    //         (n - 1) / workGroupSize + 1,
    //         hostAccessMode = HostAccessMode.NotAccessible
    //     )

    // PrefixSum.scanGeneral
    //     <@
    //         fun (_: ClArray<'a>)
    //             (_: 'a)
    //             (_: int)
    //             (_: int) ->

    //             let mutable a = 1
    //             a <- 1
    //     @>
    //     <@
    //         fun (resultBuffer: ClArray<_>)
    //             (resultLocalBuffer: 'a[])
    //             (inputArrayLength: int)
    //             (_: int)
    //             (i: int)
    //             (localID: int) ->

    //             if i < inputArrayLength then
    //                 resultBuffer.[i] <- resultLocalBuffer.[localID]
    //     @>
    //     clContext
    //     workGroupSize
    //     processor
    //     xs
    //     xs.Length
    //     vertices
    //     vertices.Length
    //     totalSum
    //     0
    //     <@ (+) @>
    //     0

    // let _ = ClArray.prefixSumExcludeInplace clContext workGroupSize processor xs totalSum
    opencl {
        let n = 100
        let workGroupSize = 256
        let! clContext = ClTask.ask
        let xs =
            clContext.CreateClArray(
                n,
                hostAccessMode = HostAccessMode.NotAccessible
            )
        let totalSum = clContext.CreateClArray(1)
        let _ = PrefixSum.runExcludeInplace clContext workGroupSize clContext.CommandQueue xs totalSum 0 <@ (+) @> 0
        return ()
    }
    |> ClTask.runSync (ClContext())

    // let res = Array.zeroCreate n
    // let _ = processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(xs, res, ch))
    // processor.Post(Msg.CreateFreeMsg<_>(xs))
    // processor.Post(Msg.CreateFreeMsg<_>(ys))

    // processor.PostAndReply <| MsgNotifyMe

    // printfn "%A" res

    0
