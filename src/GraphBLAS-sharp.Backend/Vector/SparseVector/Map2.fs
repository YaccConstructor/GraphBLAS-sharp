namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open FSharp.Quotations
open Microsoft.FSharp.Control
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Quotes

module internal Map2 =
    let private preparePositions<'a, 'b, 'c> (clContext: ClContext) workGroupSize opAdd =

        let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) length leftValuesLength rightValuesLength (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultIndices: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < length then

                    let (leftValue: 'a option) =
                        (%Search.Bin.byKey) leftValuesLength gid leftIndices leftValues

                    let (rightValue: 'b option) =
                        (%Search.Bin.byKey) rightValuesLength gid rightIndices rightValues

                    match (%op) leftValue rightValue with
                    | Some value ->
                        resultValues.[gid] <- value
                        resultIndices.[gid] <- gid

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel =
            clContext.Compile <| preparePositions opAdd

        fun (processor: MailboxProcessor<_>) (vectorLenght: int) (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) ->

            let resultBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vectorLenght)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vectorLenght)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, vectorLenght)

            let ndRange =
                Range1D.CreateValid(vectorLenght, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            vectorLenght
                            leftValues.Length
                            rightValues.Length
                            leftValues
                            leftIndices
                            rightValues
                            rightIndices
                            resultBitmap
                            resultValues
                            resultIndices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resultBitmap, resultValues, resultIndices

    let private preparePositionsSparseDense<'a, 'b, 'c> (clContext: ClContext) workGroupSize opAdd =

        let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) length (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b option>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultIndices: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < length then

                    let i = leftIndices.[gid]

                    let (leftValue: 'a option) = Some leftValues.[gid]

                    let (rightValue: 'b option) = rightValues.[i]

                    match (%op) leftValue rightValue with
                    | Some value ->
                        resultValues.[gid] <- value
                        resultIndices.[gid] <- i

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel =
            clContext.Compile <| preparePositions opAdd

        fun (processor: MailboxProcessor<_>) (vectorLenght: int) (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b option>) ->

            let resultBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vectorLenght)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vectorLenght)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, vectorLenght)

            let ndRange =
                Range1D.CreateValid(vectorLenght, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            vectorLenght
                            leftValues
                            leftIndices
                            rightValues
                            resultBitmap
                            resultValues
                            resultIndices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resultBitmap, resultValues, resultIndices

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct> (clContext: ClContext) op workGroupSize =

        let prepare =
            preparePositions<'a, 'b, 'c> clContext workGroupSize op

        let setPositions =
            Common.setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) ->

            let bitmap, allValues, allIndices =
                prepare
                    processor
                    leftVector.Size
                    leftVector.Values
                    leftVector.Indices
                    rightVector.Values
                    rightVector.Indices

            let resultValues, resultIndices =
                setPositions processor allocationMode allValues allIndices bitmap

            processor.Post(Msg.CreateFreeMsg<_>(allIndices))
            processor.Post(Msg.CreateFreeMsg<_>(allValues))
            processor.Post(Msg.CreateFreeMsg<_>(bitmap))

            { Context = clContext
              Values = resultValues
              Indices = resultIndices
              Size = max leftVector.Size rightVector.Size }

    let runSparseDense<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        op
        workGroupSize
        =

        let prepare =
            preparePositionsSparseDense<'a, 'b, 'c> clContext workGroupSize op

        let setPositions =
            Common.setPositionsNotEmpty clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector.Sparse<'a>) (rightVector: ClArray<'b option>) ->

            let bitmap, allValues, allIndices =
                prepare processor leftVector.NNZ leftVector.Values leftVector.Indices rightVector

            let resultValues, resultIndices, resultLength =
                setPositions processor allocationMode allValues allIndices bitmap

            processor.Post(Msg.CreateFreeMsg<_>(allIndices))
            processor.Post(Msg.CreateFreeMsg<_>(allValues))
            processor.Post(Msg.CreateFreeMsg<_>(bitmap))

            if resultLength > 0 then
                { Context = clContext
                  Values = resultValues
                  Indices = resultIndices
                  Size = leftVector.Size }
            else
                resultValues.Dispose processor

                { Context = clContext
                  Values = clContext.CreateClArray [| Unchecked.defaultof<'c> |]
                  Indices = resultIndices
                  Size = 1 }

    let private preparePositionsAssignByMask<'a, 'b when 'a: struct and 'b: struct>
        (clContext: ClContext)
        op
        workGroupSize
        =

        let assign op =
            <@ fun (ndRange: Range1D) length leftValuesLength rightValuesLength (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) (value: ClCell<'a>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultIndices: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                let value = value.Value

                if gid < length then

                    let (leftValue: 'a option) =
                        (%Search.Bin.byKey) leftValuesLength gid leftIndices leftValues

                    let (rightValue: 'b option) =
                        (%Search.Bin.byKey) rightValuesLength gid rightIndices rightValues

                    match (%op) leftValue rightValue value with
                    | Some value ->
                        resultValues.[gid] <- value
                        resultIndices.[gid] <- gid

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel = clContext.Compile <| assign op

        fun (processor: MailboxProcessor<_>) (vectorLenght: int) (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) (value: ClCell<'a>) ->

            let resultBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vectorLenght)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vectorLenght)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(DeviceOnly, vectorLenght)

            let ndRange =
                Range1D.CreateValid(vectorLenght, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            vectorLenght
                            leftValues.Length
                            rightValues.Length
                            leftValues
                            leftIndices
                            rightValues
                            rightIndices
                            value
                            resultBitmap
                            resultValues
                            resultIndices)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultBitmap, resultValues, resultIndices

    ///<param name="clContext">.</param>
    ///<param name="op">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let assignByMask<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) op workGroupSize =

        let prepare =
            preparePositionsAssignByMask clContext op workGroupSize

        let setPositions =
            Common.setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) (value: ClCell<'a>) ->

            let bitmap, values, indices =
                prepare
                    processor
                    leftVector.Size
                    leftVector.Values
                    leftVector.Indices
                    rightVector.Values
                    rightVector.Indices
                    value

            let resultValues, resultIndices =
                setPositions processor allocationMode values indices bitmap

            processor.Post(Msg.CreateFreeMsg<_>(indices))
            processor.Post(Msg.CreateFreeMsg<_>(values))
            processor.Post(Msg.CreateFreeMsg<_>(bitmap))

            { Context = clContext
              Values = resultValues
              Indices = resultIndices
              Size = rightVector.Size }
