namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ClContext

module Reduce =
    let private runGeneral (clContext: ClContext) workGroupSize scan scanToCell =

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let scan = scan processor

            let firstLength =
                (inputArray.Length - 1) / workGroupSize + 1

            let firstVerticesArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, firstLength)

            let secondLength = (firstLength - 1) / workGroupSize + 1

            let secondVerticesArray =
                clContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, secondLength)

            let mutable verticesArrays = firstVerticesArray, secondVerticesArray
            let swap (a, b) = (b, a)

            scan inputArray inputArray.Length (fst verticesArrays)

            let mutable verticesLength = firstLength

            while verticesLength > workGroupSize do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays

                scan fstVertices verticesLength sndVertices

                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            let fstVertices = fst verticesArrays

            let result =
                scanToCell processor fstVertices verticesLength

            processor.Post(Msg.CreateFreeMsg(firstVerticesArray))
            processor.Post(Msg.CreateFreeMsg(secondVerticesArray))

            result

    let private scanSum (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) zero =

        let subSum = SubSum.sequentialSum opAdd

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]
                else
                    localValues.[lid] <- zero

                barrierLocal ()

                (%subSum) workGroupSize lid localValues

                resultArray.[gid / workGroupSize] <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength (resultArray: ClArray<'a>) ->
            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private scanToCellSum (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) zero =

        let subSum = SubSum.sequentialSum opAdd

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultCell: ClCell<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]
                else
                    localValues.[lid] <- zero

                barrierLocal ()

                (%subSum) workGroupSize lid localValues

                resultCell.Value <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let resultCell = clContext.CreateClCell zero

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultCell))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultCell

    let sum (clContext: ClContext) workGroupSize op zero =

        let scan = scanSum clContext workGroupSize op zero

        let scanToCell =
            scanToCellSum clContext workGroupSize op zero

        let run =
            runGeneral clContext workGroupSize scan scanToCell

        fun (processor: MailboxProcessor<_>) (array: ClArray<'a>) -> run processor array

    let private scanReduce<'a when 'a: struct>
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        =

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]

                barrierLocal ()

                if gid < length then

                    (%SubReduce.run opAdd) length workGroupSize gid lid localValues

                    if lid = 0 then
                        resultArray.[gid / workGroupSize] <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength (resultArray: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let private scanToCellReduce<'a when 'a: struct>
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        =

        let scan =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) (resultValue: ClCell<'a>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localValues = localArray<'a> workGroupSize

                if gid < length then
                    localValues.[lid] <- inputArray.[gid]

                barrierLocal ()

                if gid < length then

                    (%SubReduce.run opAdd) length workGroupSize gid lid localValues

                    if lid = 0 then
                        resultValue.Value <- localValues.[0] @>

        let kernel = clContext.Compile(scan)

        fun (processor: MailboxProcessor<_>) (valuesArray: ClArray<'a>) valuesLength ->

            let ndRange =
                Range1D.CreateValid(valuesArray.Length, workGroupSize)

            let resultCell =
                clContext.CreateClCell Unchecked.defaultof<'a>

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange valuesLength valuesArray resultCell))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultCell

    let reduce (clContext: ClContext) workGroupSize op =

        let scan = scanReduce clContext workGroupSize op

        let scanToCell =
            scanToCellReduce clContext workGroupSize op

        let run =
            runGeneral clContext workGroupSize scan scanToCell

        fun (processor: MailboxProcessor<_>) (array: ClArray<'a>) -> run processor array
