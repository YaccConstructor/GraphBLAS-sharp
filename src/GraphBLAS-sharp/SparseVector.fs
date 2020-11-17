namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext

type COOVector<'a when 'a : struct and 'a : equality>(dyads: array<'a * int>, length: int) =
    inherit Vector<'a>()

    let mutable dyads = Array.distinct dyads

    override this.Length = length
    override this.AsArray = failwith "Not Implemented"

    new(length) = COOVector(Array.empty, length)
    new() = COOVector(0)

    override this.Item
        with get (mask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"

    override this.Item
        with get (idx: int) : Scalar<'a> =
            failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Item
        with set
            (mask: Mask1D)
            (Scalar (value: 'a)) =

            match mask.Length with
            | Some length ->
                if length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length length)
            | _ -> ()

            let vectorValues, vectorIndices = dyads |> Array.unzip
            let extraValues = Array.zeroCreate this.Length // it does not matter what the initial values are
            let extraIndices = Array.init this.Length (fun _ -> -1)
            let command =
                <@
                    fun (ndRange: _1D)
                        (vectorValuesBuffer: array<'a>)
                        (vectorIndicesBuffer: array<int>)
                        (extraValuesBuffer: array<'a>)
                        (extraIndicesBuffer: array<int>) ->

                        let i = ndRange.GlobalID0
                        if mask.[i] then
                            let mutable j = 0
                            while j < vectorIndicesBuffer.Length && vectorIndicesBuffer.[j] <> i do
                                j <- j + 1
                            if j < vectorIndicesBuffer.Length
                            then
                                vectorValuesBuffer.[j] <- value
                            else
                                extraValuesBuffer.[i] <- value
                                extraIndicesBuffer.[i] <- i
                @>

            let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
            let ndRange = _1D(this.Length)
            kernelPrepare
                ndRange
                vectorValues
                vectorIndices
                extraValues
                extraIndices
            currentContext.CommandQueue.Add(kernelRun ()) |> ignore
            currentContext.CommandQueue.Add(vectorValues.ToHost currentContext.Provider) |> ignore
            currentContext.CommandQueue.Add(extraValues.ToHost currentContext.Provider) |> ignore
            currentContext.CommandQueue.Add(extraIndices.ToHost currentContext.Provider) |> ignore
            currentContext.CommandQueue.Finish() |> ignore

            dyads <-
                Array.append (Array.zip vectorValues vectorIndices) << Array.filter (snd >> (<>) -1) <| Array.zip extraValues extraIndices
            ()

    member internal this.VxCOOm
        (matrix: COOMatrix<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) : Vector<'a> =

        let matrixColumnCount = matrix.ColumnCount

        let plus = !> semiring.PlusMonoid.Append
        let mult = !> semiring.Times
        let zero = semiring.PlusMonoid.Zero

        let vectorValues, vectorIndices = Array.unzip dyads
        let matrixValues, matrixRows, matrixColumns = Array.unzip3 matrix.Triples
        let resultValues = Array.init matrixColumnCount (fun _ -> zero)
        let resultIndices = Array.init matrixColumnCount id
        let command =
            <@
                fun (ndRange: _1D)
                    (resultValuesBuffer: array<'a>)
                    (matrixValuesBuffer: array<'a>)
                    (matrixRowsBuffer: array<int>)
                    (matrixColumnsBuffer: array<int>)
                    (vectorValuesBuffer: array<'a>)
                    (vectorIndicesBuffer: array<int>) ->

                    let i = ndRange.GlobalID0
                    if mask.[i] then
                        let mutable localResultBuffer = resultValuesBuffer.[i]
                        for j in 0 .. vectorValuesBuffer.Length - 1 do
                            for k in 0 .. matrixValuesBuffer.Length - 1 do
                                if vectorIndicesBuffer.[j] = matrixRowsBuffer.[k] && matrixColumnsBuffer.[k] = i then
                                    localResultBuffer <- (%plus) localResultBuffer
                                        ((%mult) vectorValuesBuffer.[j] matrixValuesBuffer.[k])
                        resultValuesBuffer.[i] <- localResultBuffer
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
        let ndRange = _1D(matrixColumnCount)
        kernelPrepare
            ndRange
            resultValues
            matrixValues
            matrixRows
            matrixColumns
            vectorValues
            vectorIndices
        currentContext.CommandQueue.Add(kernelRun ()) |> ignore
        currentContext.CommandQueue.Add(resultValues.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Finish() |> ignore

        // remove zero values
        let resultDyads = Array.filter (fst >> (<>) zero) <| Array.zip resultValues resultIndices

        upcast COOVector(resultDyads, matrixColumnCount)

    override this.Vxm
        (matrix: Matrix<'a>)
        (mask: Mask1D)
        (semiring: Semiring<'a>) =

        if matrix.RowCount <> this.Length then
            invalidArg
                "matrix"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length matrix.RowCount)

        match mask.Length with
            | Some length ->
                if length <> this.Length then
                    invalidArg
                        "mask"
                        (sprintf "Argument has invalid dimension. Need %i, but given %i" this.Length length)
            | _ -> ()

        match matrix with
            | :? COOMatrix<'a> -> this.VxCOOm (downcast matrix) mask semiring
            | _ -> failwith "Not Implemented"

    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"

    override this.Reduce
        (monoid: Monoid<'a>) =

        let nonZeroValuesCount = dyads.Length

        let mutable expNonZeroValuesCount = 1
        let mutable exponent = 0
        while expNonZeroValuesCount < nonZeroValuesCount do
            expNonZeroValuesCount <- expNonZeroValuesCount * 2
            exponent <- exponent + 1

        let plus = !> monoid.Append
        let zero = monoid.Zero

        let values =
            dyads |> Array.unzip |> fst |> Array.append <| Array.init (expNonZeroValuesCount - nonZeroValuesCount) (fun _ -> zero)

        let mutable middleIndex = expNonZeroValuesCount / 2
        let command =
            <@
                fun (ndRange: _1D)
                    (valuesBuffer: array<'a>) ->

                let i = ndRange.GlobalID0
                valuesBuffer.[i] <- (%plus) valuesBuffer.[i] valuesBuffer.[i + middleIndex]
            @>

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command

        for i in 0 .. exponent - 1 do
            let ndRange = _1D(middleIndex - 1)
            kernelPrepare
                ndRange
                values
            currentContext.CommandQueue.Add(kernelRun ()) |> ignore
            currentContext.CommandQueue.Add(values.ToHost currentContext.Provider) |> ignore
            currentContext.CommandQueue.Finish() |> ignore

            middleIndex <- middleIndex / 2

        Scalar values.[0]

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
