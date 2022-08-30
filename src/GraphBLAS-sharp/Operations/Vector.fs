namespace GraphBLAS.FSharp

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =

    (*
        constructors
    *)

    let build (size: int) (indices: int []) (values: 'a []) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofTuples (size: int) (tuples: VectorTuples<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let ofList (size: int) (elements: (int * 'a) list) : GraphblasEvaluation<Vector<'a>> =
        let (indices, values) =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        graphblas {
            return
                VectorCOO
                <| COOVector.FromTuples(size, indices, values)
        }

    // можно оставить, но с условием, что будет создаваться full vector
    // let ofArray (array: 'a[]) : GraphblasEvaluation<Vector<'a>> =
    //     failwith "Not Implemented yet"

    let init (size: int) (initializer: int -> 'a) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    let create (size: int) (value: 'a) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    let zeroCreate<'a when 'a: struct> (size: int) : GraphblasEvaluation<Vector<'a>> =
        graphblas {
            return
                VectorCOO
                <| COOVector.FromTuples(size, [||], [||])
        }

    (*
        methods
    *)

    let size (vector: Vector<'a>) : int = failwith "Not Implemented yet"
    let copy (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"
    let resize (size: int) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> = failwith "Not Implemented yet"

    // NOTE int cant be sync
    let nnz (vector: Vector<'a>) : GraphblasEvaluation<int> = failwith "Not Implemented yet"

    let tuples (vector: Vector<'a>) : GraphblasEvaluation<VectorTuples<'a>> =
        failwith "Not Implemented yet"
//        match vector with
//        | VectorCOO vector ->
//            opencl {
//                if vector.Values.Length = 0 then
//                    return { Indices = [||]; Values = [||] }
//                else
//                    let! ind = Copy.copyArray vector.Indices
//                    let! vals = Copy.copyArray vector.Values
//
//                    return { Indices = ind; Values = vals }
//            }
//        |> EvalGB.fromCl

    let mask (vector: Vector<'a>) : GraphblasEvaluation<Mask1D> =
        failwith "Not Implemented yet"
//        match vector with
//        | VectorCOO vector ->
//            opencl {
//                let! indices = Copy.copyArray vector.Indices
//                return Mask1D(indices, vector.Size, false)
//            }
//        |> EvalGB.fromCl

    let complemented (vector: Vector<'a>) : GraphblasEvaluation<Mask1D> =
        failwith "Not Implemented yet"
//        match vector with
//        | VectorCOO vector ->
//            opencl {
//                let! indices = Copy.copyArray vector.Indices
//
//                let! complementedMask =
//                    Mask.GetComplemented.mask1D
//                    <| Mask1D(indices, vector.Size, true)
//
//                return complementedMask
//            }
//        |> EvalGB.fromCl

    let switch (vectorFormat: VectorFormat) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let synchronize (vector: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"
//        match vector with
//        | VectorCOO vector ->
//            opencl {
//                let! _ =
//                    if vector.Indices.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        ToHost vector.Indices
//
//                let! _ =
//                    if vector.Values.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        ToHost vector.Values
//
//                return ()
//            }
//        |> EvalGB.fromCl

    let synchronizeAndReturn (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"
//        match vector with
//        | VectorCOO vector ->
//            opencl {
//                let! _ =
//                    if vector.Indices.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        ToHost vector.Indices
//
//                let! _ =
//                    if vector.Values.Length = 0 then
//                        opencl { return [||] }
//                    else
//                        ToHost vector.Values
//
//                return VectorCOO vector
//            }
//        |> EvalGB.fromCl

    (*
        assignment, extraction and filling
    *)

    /// vec.[mask]
    let extractSubVector (vector: Vector<'a>) (mask: Mask1D) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    /// vec.[idx]
    let extractValue (vector: Vector<'a>) (idx: int) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"

    // assignToVector
    /// t <- vec
    let assignVector (target: Vector<'a>) (source: Vector<'a>) : GraphblasEvaluation<unit> =
        if target.Size <> source.Size then
            invalidArg "source"
            <| sprintf "The size of source vector must be %A. Received: %A" target.Size source.Size

        match source, target with
        | VectorCOO source, VectorCOO target ->
            opencl {
                target.Indices <- source.Indices
                target.Values <- source.Values
            }
        |> EvalGB.fromCl

    /// t.[mask] <- vec
    let assignSubVector (target: Vector<'a>) (mask: Mask1D) (source: Vector<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"
//        if target.Size <> mask.Size then
//            invalidArg "mask"
//            <| sprintf "The size of mask must be %A. Received: %A" target.Size mask.Size
//
//        if target.Size <> source.Size then
//            invalidArg "source"
//            <| sprintf "The size of source vector must be %A. Received: %A" target.Size source.Size
//
//        match source, target, mask with
//        | VectorCOO source, VectorCOO target, mask when not mask.IsComplemented ->
//            opencl {
//                let! (resultIndices, resultValues) =
//                    COOVector.AssignSubVector.run target.Indices target.Values source.Indices source.Values mask.Indices
//
//                target.Indices <- resultIndices
//                target.Values <- resultValues
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    /// t.[idx] <- value
    let assignValue (target: Vector<'a>) (idx: int) (value: Scalar<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// vec.[*] <- value
    let fillVector (vector: Vector<'a>) (value: Scalar<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"

    /// vec.[mask] <- value
    let fillSubVector (vector: Vector<'a>) (mask: Mask1D) (value: Scalar<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"
//        match vector, value, mask with
//        | VectorCOO vector, ScalarWrapped scalar, mask when not mask.IsComplemented ->
//            opencl {
//                let! (resultIndices, resultValues) =
//                    COOVector.FillSubVector.run vector.Indices vector.Values mask.Indices scalar.Value
//
//                vector.Indices <- resultIndices
//                vector.Values <- resultValues
//            }
//        | _ -> failwith "Not Implemented"
//        |> EvalGB.fromCl

    (*
        operations
    *)

    let eWiseAdd
        (monoid: IMonoid<'a>)
        (leftVector: Vector<'a>)
        (rightVector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let eWiseMult
        (semiring: ISemiring<'a>)
        (leftVector: Vector<'a>)
        (rightVector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let apply (mapper: UnaryOp<'a, 'b>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'b>> =
        failwith "Not Implemented yet"

    let select (predicate: UnaryOp<'a, bool>) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let reduce (monoid: IMonoid<'a>) (vector: Vector<'a>) : GraphblasEvaluation<Scalar<'a>> =
        failwith "Not Implemented yet"
//        let (ClosedBinaryOp plus) = monoid.Plus
//
//        match vector with
//        | VectorCOO vector ->
//            opencl {
//                let! result = Sum.run vector.Values plus monoid.Zero
//                return ScalarWrapped { Value = result }
//            }
//        |> EvalGB.fromCl

    let eWiseAddWithMask
        (monoid: IMonoid<'a>)
        (mask: Mask1D)
        (leftVector: Vector<'a>)
        (rightVector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let eWiseMultWithMask
        (semiring: ISemiring<'a>)
        (mask: Mask1D)
        (leftVector: Vector<'a>)
        (rightVector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

    let applyWithMask (mapper: UnaryOp<'a, 'b>) (mask: Mask1D) (vector: Vector<'a>) : GraphblasEvaluation<Vector<'b>> =
        failwith "Not Implemented yet"

    let selectWithMask
        (predicate: UnaryOp<'a, bool>)
        (mask: Mask1D)
        (vector: Vector<'a>)
        : GraphblasEvaluation<Vector<'a>> =
        failwith "Not Implemented yet"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module VectorTuples =
    let synchronize (vectorTuples: VectorTuples<'a>) =
        failwith "Not Implemented yet"
//        opencl {
//            let! _ =
//                if vectorTuples.Indices.Length = 0 then
//                    opencl { return [||] }
//                else
//                    ToHost vectorTuples.Indices
//
//            let! _ =
//                if vectorTuples.Values.Length = 0 then
//                    opencl { return [||] }
//                else
//                    ToHost vectorTuples.Values
//
//            return ()
//        }
//        |> EvalGB.fromCl

    let synchronizeAndReturn (vectorTuples: VectorTuples<'a>) =
        failwith "Not Implemented yet"
//        opencl {
//            let! _ =
//                if vectorTuples.Indices.Length = 0 then
//                    opencl { return [||] }
//                else
//                    ToHost vectorTuples.Indices
//
//            let! _ =
//                if vectorTuples.Values.Length = 0 then
//                    opencl { return [||] }
//                else
//                    ToHost vectorTuples.Values
//
//            return vectorTuples
//        }
//        |> EvalGB.fromCl
