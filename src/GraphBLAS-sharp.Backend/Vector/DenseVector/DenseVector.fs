namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module DenseVector =
    let zeroCreate (clContext: ClContext) (workGroupSize: int)  =
        let zeroCreate = ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (length: int) ->
            let resultValues = zeroCreate processor length

            resultValues :?> ClDenseVector<'a>

    let ofList (clContext: ClContext) (workGroupSize: int) (elements: (int * 'a) list) =
        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let toOptionArray = ClArray.toOptionArray clContext workGroupSize

        fun (processor: MailboxProcessor<_>) ->
            let values = clContext.CreateClArray values
            let indices = clContext.CreateClArray indices

            toOptionArray processor values indices elements.Length :?> ClDenseVector<'a>


    let copy (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->
            copy processor vector :?> ClDenseVector<'a>

    let private copyWithValue (clContext: ClContext) (workGroupSize: int) =

        let fillVector =
            <@ fun (ndRange: Range1D) length (maskArray: ClArray<'a option>) (scalar: ClCell<'b>) (resultArray: ClArray<'b option>)->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match maskArray.[gid] with
                        | Some _ ->
                            resultArray.[gid] <- Some scalar.Value
                        | None ->
                            resultArray.[gid] <- None @>

        let kernel = clContext.Compile(fillVector)

        fun (processor: MailboxProcessor<_>) (maskVector: ClDenseVector<'a>) (scalar: 'b) ->

            let resultArray =
                clContext.CreateClArray(
                    maskVector.Size,
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

            let ndRange = Range1D.CreateValid(maskVector.Size, workGroupSize)

            let kernel = kernel.GetKernel ()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            maskVector.Size
                            maskVector
                            clScalar
                            resultArray)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
            processor.Post(Msg.CreateFreeMsg<_>(clScalar))

            resultArray :?> ClDenseVector<'b>

    let elementWiseAddAtLeasOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) (workGroupSize: int) =

        let eWiseAdd =
            <@ fun (ndRange: Range1D) leftVectorLength rightVectorLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                 let gid = ndRange.GlobalID0

                 let mutable leftItem = None
                 let mutable rightItem = None

                 if gid < leftVectorLength then
                    leftItem <- leftVector[gid]

                 if gid < rightVectorLength then
                    rightItem <- rightVector[gid]

                 match leftItem, rightItem with
                 | Some left, Some right ->
                    resultVector.[gid] <- (%opAdd) (Both (left, right))
                 | Some left, None ->
                    resultVector.[gid] <- (%opAdd) (Left left)
                 | None, Some right ->
                    resultVector.[gid] <- (%opAdd) (Right right)
                 | None, None ->
                    resultVector.[gid] <- None @>

        let kernel = clContext.Compile(eWiseAdd)

        fun (processor: MailboxProcessor<_>) (leftVector: ClDenseVector<'a>) (rightVector: ClDenseVector<'b>) ->

            let resultVector =
                clContext.CreateClArray(
                    leftVector.Size,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultLength = max leftVector.Size rightVector.Size

            let ndRange = Range1D.CreateValid (resultLength, workGroupSize)

            let kernel = kernel.GetKernel ()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftVector.Size
                            rightVector.Size
                            leftVector
                            rightVector
                            resultVector)
                )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let fillSubVector (clContext: ClContext) (workGroupSize: int) =

        let opAdd = VectorOperations.fillSubAddAtLeastOne None

        let eWiseAdd = elementWiseAddAtLeasOne clContext opAdd workGroupSize

        let copyWithValue = copyWithValue clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClDenseVector<'a>) (maskVector: ClDenseVector<'b>) (scalar: 'a) ->

           let maskVector = copyWithValue processor maskVector scalar

           let resultVector =
               eWiseAdd processor leftVector maskVector

           processor.Post(Msg.CreateFreeMsg<_>(maskVector))

           resultVector

    let Complemented (clContext: ClContext) (workGroupSize: int) =

        let complemented =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a option>) (resultArray: ClArray<'a option>) ->

                 let gid = ndRange.GlobalID0

                 if gid < length then
                    match inputArray.[gid] with
                    | Some _ ->
                        resultArray.[gid] <- None
                    | None ->
                        resultArray.[gid] <- Some Unchecked.defaultof<'a> @>


        let kernel = clContext.Compile(complemented)

        fun (processor: MailboxProcessor<_>) (vector: ClDenseVector<'a>) ->

            let length = vector.Size

            let resultArray =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = kernel.GetKernel ()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            length
                            vector
                            resultArray)
                )

            resultArray :?> ClDenseVector<'a>
