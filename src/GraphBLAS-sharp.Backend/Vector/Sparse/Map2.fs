namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open FSharp.Quotations
open Microsoft.FSharp.Control
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVector
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Quotes

module internal Map2 =
    let private preparePositions<'a, 'b, 'c> opAdd (clContext: ClContext) workGroupSize =

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

    let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct> op (clContext: ClContext) workGroupSize =

        let prepare =
            preparePositions<'a, 'b, 'c> op clContext workGroupSize

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

    let private preparePositionsAssignByMask<'a, 'b when 'a: struct and 'b: struct>
        op
        (clContext: ClContext)
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
    let assignByMask<'a, 'b when 'a: struct and 'b: struct> op (clContext: ClContext) workGroupSize =

        let prepare =
            preparePositionsAssignByMask op clContext workGroupSize

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

    module AtLeastOne =
        let private preparePositions<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
            op
            (clContext: ClContext)
            workGroupSize
            =

            let preparePositions opAdd =
                <@ fun (ndRange: Range1D) length (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) (allValues: ClArray<'c>) (positions: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < length - 1
                       && allIndices.[gid] = allIndices.[gid + 1] then
                        let result =
                            (%opAdd) (Some leftValues.[gid]) (Some rightValues.[gid + 1])

                        (%PreparePositions.both) gid result positions allValues
                    elif (gid < length
                          && gid > 0
                          && allIndices.[gid - 1] <> allIndices.[gid])
                         || gid = 0 then
                        let leftResult = (%opAdd) (Some leftValues.[gid]) None
                        let rightResult = (%opAdd) None (Some rightValues.[gid])

                        (%PreparePositions.leftRight) gid leftResult rightResult isLeft allValues positions @>

            let kernel = clContext.Compile <| preparePositions op

            fun (processor: MailboxProcessor<_>) (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) ->

                let length = allIndices.Length

                let allValues =
                    clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, length)

                let positions =
                    clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, length)

                let ndRange =
                    Range1D.CreateValid(length, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                length
                                allIndices
                                leftValues
                                rightValues
                                isLeft
                                allValues
                                positions)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(kernel))

                allValues, positions

        ///<param name="clContext">.</param>
        ///<param name="op">.</param>
        ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct> op (clContext: ClContext) workGroupSize =

            let merge = Merge.run clContext workGroupSize

            let prepare =
                preparePositions<'a, 'b, 'c> op clContext workGroupSize

            let setPositions =
                Common.setPositions clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) ->

                let allIndices, leftValues, rightValues, isLeft = merge processor leftVector rightVector

                let allValues, positions =
                    prepare processor allIndices leftValues rightValues isLeft

                processor.Post(Msg.CreateFreeMsg<_>(leftValues))
                processor.Post(Msg.CreateFreeMsg<_>(rightValues))
                processor.Post(Msg.CreateFreeMsg<_>(isLeft))

                let resultValues, resultIndices =
                    setPositions processor allocationMode allValues allIndices positions

                processor.Post(Msg.CreateFreeMsg<_>(allIndices))
                processor.Post(Msg.CreateFreeMsg<_>(allValues))
                processor.Post(Msg.CreateFreeMsg<_>(positions))

                { Context = clContext
                  Values = resultValues
                  Indices = resultIndices
                  Size = max leftVector.Size rightVector.Size }
