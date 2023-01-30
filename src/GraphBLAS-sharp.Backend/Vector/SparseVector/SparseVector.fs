namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClContext

module SparseVector =

    let private setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let valuesScatter =
            Scatter.runInplace clContext workGroupSize

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

        let resultLength = Array.zeroCreate<int> 1

        fun (processor: MailboxProcessor<_>) flag (allValues: ClArray<'a>) (allIndices: ClArray<int>) (positions: ClArray<int>) ->

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = sum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultValues =
                clContext.CreateClArrayWithFlag<'a>(flag, resultLength)

            let resultIndices =
                clContext.CreateClArrayWithFlag<int>(flag, resultLength)

            valuesScatter processor positions allValues resultValues

            indicesScatter processor positions allIndices resultIndices

            resultValues, resultIndices


    let preparePositionsGeneral<'a, 'b, 'c> (clContext: ClContext) workGroupSize opAdd =

        let kernel =
            clContext.Compile
            <| Elementwise.preparePositionsGen opAdd

        fun (processor: MailboxProcessor<_>) (vectorLenght: int) (leftValues: ClArray<'a>) (leftIndices: ClArray<int>) (rightValues: ClArray<'b>) (rightIndices: ClArray<int>) ->

            let resultBitmap =
                clContext.CreateClArrayWithFlag<int>(DeviceOnly, vectorLenght)

            let resultIndices =
                clContext.CreateClArrayWithFlag<int>(DeviceOnly, vectorLenght)

            let resultValues =
                clContext.CreateClArrayWithFlag<'c>(DeviceOnly, vectorLenght)

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

    let elementwiseGeneral<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        op
        workGroupSize
        =

        let prepare =
            preparePositionsGeneral<'a, 'b, 'c> clContext workGroupSize op

        let setPositions = setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) ->

            let bitmap, allValues, allIndices =
                prepare
                    processor
                    leftVector.Size
                    leftVector.Values
                    leftVector.Indices
                    rightVector.Values
                    rightVector.Indices

            let resultValues, resultIndices =
                setPositions processor flag allValues allIndices bitmap

            processor.Post(Msg.CreateFreeMsg<_>(allIndices))
            processor.Post(Msg.CreateFreeMsg<_>(allValues))
            processor.Post(Msg.CreateFreeMsg<_>(bitmap))

            { Context = clContext
              Values = resultValues
              Indices = resultIndices
              Size = max leftVector.Size rightVector.Size }

    let private merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let kernel =
            clContext.Compile(Elementwise.merge workGroupSize)

        fun (processor: MailboxProcessor<_>) flag (firstIndices: ClArray<int>) (firstValues: ClArray<'a>) (secondIndices: ClArray<int>) (secondValues: ClArray<'b>) ->

            let firstSide = firstIndices.Length

            let secondSide = secondIndices.Length

            let sumOfSides =
                firstIndices.Length + secondIndices.Length

            let allIndices =
                clContext.CreateClArrayWithFlag<int>(DeviceOnly, sumOfSides)

            let firstResultValues =
                clContext.CreateClArrayWithFlag<'a>(DeviceOnly, sumOfSides)

            let secondResultValues =
                clContext.CreateClArrayWithFlag<'b>(DeviceOnly, sumOfSides)

            let isLeftBitmap =
                clContext.CreateClArrayWithFlag<int>(DeviceOnly, sumOfSides)

            let ndRange =
                Range1D.CreateValid(sumOfSides, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstSide
                            secondSide
                            sumOfSides
                            firstIndices
                            firstValues
                            secondIndices
                            secondValues
                            allIndices
                            firstResultValues
                            secondResultValues
                            isLeftBitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allIndices, firstResultValues, secondResultValues, isLeftBitmap

    let private preparePositions<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        op
        workGroupSize
        =

        let kernel =
            clContext.Compile(Elementwise.preparePositions op)

        fun (processor: MailboxProcessor<_>) (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) ->

            let length = allIndices.Length

            let allValues =
                clContext.CreateClArrayWithFlag<'c>(DeviceOnly, length)

            let positions =
                clContext.CreateClArrayWithFlag<int>(DeviceOnly, length)

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc ndRange length allIndices leftValues rightValues isLeft allValues positions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allValues, positions

    ///<param name="clContext">.</param>
    ///<param name="op">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let elementwise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct> (clContext: ClContext) op workGroupSize =

        let merge = merge clContext workGroupSize

        let prepare =
            preparePositions<'a, 'b, 'c> clContext op workGroupSize

        let setPositions = setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) ->

            let allIndices, leftValues, rightValues, isLeft =
                merge processor flag leftVector.Indices leftVector.Values rightVector.Indices rightVector.Values

            let allValues, positions =
                prepare processor allIndices leftValues rightValues isLeft

            processor.Post(Msg.CreateFreeMsg<_>(leftValues))
            processor.Post(Msg.CreateFreeMsg<_>(rightValues))
            processor.Post(Msg.CreateFreeMsg<_>(isLeft))

            let resultValues, resultIndices =
                setPositions processor flag allValues allIndices positions

            processor.Post(Msg.CreateFreeMsg<_>(allIndices))
            processor.Post(Msg.CreateFreeMsg<_>(allValues))
            processor.Post(Msg.CreateFreeMsg<_>(positions))

            { Context = clContext
              Values = resultValues
              Indices = resultIndices
              Size = max leftVector.Size rightVector.Size }

    let elementWiseAtLeastOne (clContext: ClContext) opAdd workGroupSize flag =
        elementwise clContext (Convert.atLeastOneToOption opAdd) workGroupSize flag

    let private preparePositionsFillSubVector<'a, 'b when 'a: struct and 'b: struct>
        (clContext: ClContext)
        op
        workGroupSize
        =

        let kernel =
            clContext.Compile(Elementwise.prepareFillVector op)

        fun (processor: MailboxProcessor<_>) (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (value: ClCell<'a>) (isLeft: ClArray<int>) ->

            let length = allIndices.Length

            let allValues =
                clContext.CreateClArrayWithFlag<'a>(DeviceOnly, length)

            let positions =
                clContext.CreateClArrayWithFlag(DeviceOnly, length)

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
                            value
                            isLeft
                            allValues
                            positions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allValues, positions

    ///<param name="clContext">.</param>
    ///<param name="op">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) op workGroupSize =

        let merge = merge clContext workGroupSize

        let prepare =
            preparePositionsFillSubVector clContext op workGroupSize

        let setPositions = setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) (value: ClCell<'a>) ->

            let allIndices, leftValues, rightValues, isLeft =
                merge processor flag leftVector.Indices leftVector.Values rightVector.Indices rightVector.Values

            let allValues, positions =
                prepare processor allIndices leftValues rightValues value isLeft

            processor.Post(Msg.CreateFreeMsg<_>(leftValues))
            processor.Post(Msg.CreateFreeMsg<_>(rightValues))
            processor.Post(Msg.CreateFreeMsg<_>(isLeft))

            let resultValues, resultIndices =
                setPositions processor flag allValues allIndices positions

            processor.Post(Msg.CreateFreeMsg<_>(allIndices))
            processor.Post(Msg.CreateFreeMsg<_>(allValues))
            processor.Post(Msg.CreateFreeMsg<_>(positions))

            { Context = clContext
              Values = resultValues
              Indices = resultIndices
              Size = max leftVector.Size rightVector.Size }

    let toDense (clContext: ClContext) workGroupSize =

        let toDense =
            <@ fun (ndRange: Range1D) length (values: ClArray<'a>) (indices: ClArray<int>) (resultArray: ClArray<'a option>) ->
                let gid = ndRange.GlobalID0

                if gid < length then
                    let index = indices.[gid]

                    resultArray.[index] <- Some values.[gid] @>

        let kernel = clContext.Compile(toDense)

        let create =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) flag (vector: ClVector.Sparse<'a>) ->
            let resultVector = create processor flag vector.Size

            let ndRange =
                Range1D.CreateValid(vector.Indices.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc ndRange vector.Indices.Length vector.Values vector.Indices resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let reduce<'a when 'a: struct> (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =

        let reduce =
            Reduce.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector.Sparse<'a>) -> reduce processor vector.Values
