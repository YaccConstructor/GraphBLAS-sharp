namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module DenseVector =

    let private maskWithValue (clContext: ClContext) (workGroupSize: int) = //TODO()

        let fillVector =
            <@
               fun (ndRange: Range1D) length (maskArray: ClArray<'a option>) (scalar: ClCell<'b>) (resultArray: ClArray<'b option>)->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match maskArray[gid] with
                        | Some _ ->
                            resultArray[gid] <- Some scalar.Value
                        | None ->
                            resultArray[gid] <- None
            @>

        let kernel = clContext.Compile(fillVector)

        fun (processor: MailboxProcessor<_>) (maskVector: ClArray<'a option>) (scalar: 'b) ->

            let resultArray =
                clContext.CreateClArray(
                    maskVector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let clScalar =
                clContext.CreateClCell(
                    scalar,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadOnly,
                    allocationMode = AllocationMode.Default
                    )

            let ndRange = Range1D.CreateValid(maskVector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            maskVector.Length
                            maskVector
                            clScalar
                            resultArray)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            processor.Post(Msg.CreateFreeMsg<_>(clScalar))

            resultArray

    let elementWiseAddAtLeasOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        (workGroupSize: int)
        =

        let eWiseAdd =
            <@
               fun (ndRange: Range1D) leftVectorLength rightVectorLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                 let gid = ndRange.GlobalID0

                 let mutable leftItem = None
                 let mutable rightItem = None

                 if gid < leftVectorLength then
                    leftItem <- leftVector[gid]

                 if gid < rightVectorLength then
                    rightItem <- rightVector[gid]

                 match leftItem, rightItem with
                 | Some left, Some right ->
                    resultVector[gid] <- (%opAdd) (Both (left, right))
                 | Some left, None ->
                    resultVector[gid] <- (%opAdd) (Left left)
                 | None, Some right ->
                    resultVector[gid] <- (%opAdd) (Right right)
                 | None, None ->
                    resultVector[gid] <- None
            @>

        let kernel = clContext.Compile(eWiseAdd)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->

            let resultVector =
                clContext.CreateClArray(
                    leftVector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultLength = max leftVector.Length rightVector.Length

            let ndRange = Range1D.CreateValid (resultLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftVector.Length
                            rightVector.Length
                            leftVector
                            rightVector
                            resultVector)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let fillSubVector (clContext: ClContext) (workGroupSize: int) =

        let opAdd = VectorOperations.fillSubAddAtLeastOne None

        let eWiseAdd = elementWiseAddAtLeasOne clContext opAdd workGroupSize

        let copyWithValue = maskWithValue clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (scalar: 'a) ->

           let maskVector = copyWithValue processor maskVector scalar

           let resultVector =
               eWiseAdd processor leftVector maskVector

           processor.Post(Msg.CreateFreeMsg<_>(maskVector))

           resultVector

    let complemented<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let complemented =
            <@
                fun (ndRange: Range1D) length (inputArray: ClArray<'a option>) (resultArray: ClArray<'a option>) ->

                    let gid = ndRange.GlobalID0

                    if gid < length then
                        match inputArray[gid] with
                        | Some _ ->
                            resultArray[gid] <- None
                        | None ->
                            resultArray[gid] <- Some Unchecked.defaultof<'a>
            @>


        let kernel = clContext.Compile(complemented)

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let length = vector.Length

            let resultArray =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            length
                            vector
                            resultArray)
                )

            resultArray

    let getSomeBitmap<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let getSomeBitmap =
            <@
                fun (ndRange: Range1D) length (vector: ClArray<'a option>) (positions: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < length then
                        match vector[gid] with
                        | Some _ ->
                            positions[gid] <- 1
                        | None ->
                            positions[gid] <- 0
            @>

        let kernel = clContext.Compile(getSomeBitmap)

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let positions =
                clContext.CreateClArray(
                    vector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            vector.Length
                            vector
                            positions))

            processor.Post(Msg.CreateRunMsg(kernel))

            positions

    let unzip<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let unzip =
            <@
                fun (ndRange: Range1D) length (denseVector: ClArray<'a option>) (prefixSumBuffer: ClArray<int>) (bitmap: ClArray<int>) (resultValues: ClArray<'a>) (resultIndices: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < length && bitmap[gid] = 1 then
                        let index = prefixSumBuffer[gid]

                        match denseVector[gid] with
                        | Some value ->
                            resultValues[index] <- value
                            resultIndices[index] <- gid
                        | None -> ()
            @>


        let kernel = clContext.Compile(unzip)

        let getBitmap = getSomeBitmap clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize

        let prefixSum = ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let bitmap = getBitmap processor vector

            let prefixSumArray = copy processor bitmap

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = prefixSum processor prefixSumArray resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res[0]

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default)

            let resultIndices =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default)

            let ndRange = Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            vector.Length
                            vector
                            prefixSumArray
                            bitmap
                            resultValues
                            resultIndices)
                )

            processor.Post(Msg.CreateRunMsg(kernel))

            processor.Post(Msg.CreateFreeMsg<_>(bitmap))
            processor.Post(Msg.CreateFreeMsg<_>(prefixSumArray))

            resultValues, resultIndices

    let toCoo (clContext: ClContext) (workGroupSize: int) =

        let unzip = unzip clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let values, indices = unzip processor vector

            { ClCooVector.Context = clContext
              Indices = indices
              Values = values
              Size = vector.Length }


    let reduce
        (clContext: ClContext)
        (workGroupSize: int)
        (opAdd: Expr<'a -> 'a -> 'a>)
        =

        let unzip = unzip clContext workGroupSize

        let reduce = Reduce.run clContext workGroupSize opAdd Unchecked.defaultof<'a> //TODO()

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let values, indices = unzip processor vector

            processor.Post(Msg.CreateFreeMsg<_>(indices))

            reduce processor values