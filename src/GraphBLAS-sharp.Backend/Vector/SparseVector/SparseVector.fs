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
open GraphBLAS.FSharp.Backend.Objects.ClCell

module SparseVector =

    let private setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let valuesScatter =
            Scatter.runInplace clContext workGroupSize

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (allValues: ClArray<'a>) (allIndices: ClArray<int>) (positions: ClArray<int>) ->

            let resultLength =
                (sum processor positions).ToHostAndFree(processor)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(allocationMode, resultLength)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

            valuesScatter processor positions allValues resultValues

            indicesScatter processor positions allIndices resultIndices

            resultValues, resultIndices


    let preparePositionsGeneral<'a, 'b, 'c> (clContext: ClContext) workGroupSize opAdd =

        let kernel =
            clContext.Compile
            <| Map2.preparePositionsGeneral opAdd

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

    let map2General<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct> (clContext: ClContext) op workGroupSize =

        let prepare =
            preparePositionsGeneral<'a, 'b, 'c> clContext workGroupSize op

        let setPositions = setPositions clContext workGroupSize

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

    let private merge<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) workGroupSize =

        let kernel =
            clContext.Compile(Map2.merge workGroupSize)

        fun (processor: MailboxProcessor<_>) (firstIndices: ClArray<int>) (firstValues: ClArray<'a>) (secondIndices: ClArray<int>) (secondValues: ClArray<'b>) ->

            let firstSide = firstIndices.Length

            let secondSide = secondIndices.Length

            let sumOfSides =
                firstIndices.Length + secondIndices.Length

            let allIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

            let firstResultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(DeviceOnly, sumOfSides)

            let secondResultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'b>(DeviceOnly, sumOfSides)

            let isLeftBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, sumOfSides)

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
            clContext.Compile(Map2.preparePositions op)

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
                        kernel.KernelFunc ndRange length allIndices leftValues rightValues isLeft allValues positions)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allValues, positions

    ///<param name="clContext">.</param>
    ///<param name="op">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map2<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct> (clContext: ClContext) op workGroupSize =

        let merge = merge clContext workGroupSize

        let prepare =
            preparePositions<'a, 'b, 'c> clContext op workGroupSize

        let setPositions = setPositions clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector.Sparse<'a>) (rightVector: ClVector.Sparse<'b>) ->

            let allIndices, leftValues, rightValues, isLeft =
                merge processor leftVector.Indices leftVector.Values rightVector.Indices rightVector.Values

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

    let map2AtLeastOne (clContext: ClContext) opAdd workGroupSize allocationMode =
        map2 clContext (Convert.atLeastOneToOption opAdd) workGroupSize allocationMode

    let private preparePositionsAssignByMask<'a, 'b when 'a: struct and 'b: struct>
        (clContext: ClContext)
        op
        workGroupSize
        =

        let kernel = clContext.Compile(Map2.prepareAssign op)

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

        let setPositions = setPositions clContext workGroupSize

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

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector.Sparse<'a>) ->
            let resultVector =
                create processor allocationMode vector.Size

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
