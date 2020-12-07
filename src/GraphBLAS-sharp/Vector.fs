namespace GraphBLAS.FSharp

[<AutoOpen>]
module VectorExtensions =
    type Vector<'a when 'a : struct and 'a : equality> with
        static member Sparse(denseVector: 'T[], zero: 'T) : Vector<'T> =
            failwith "Not Implemented"
        static member Sparse(length: int, initializer: int -> 'T) : Vector<'T> =
            failwith "Not Implemented"
        static member ZeroSparse(length: int) : Vector<'T> =
            failwith "Not Implemented"

        static member Dense(denseVector: 'T[], monoid: Monoid<'T>) : Vector<'T> =
            failwith "Not Implemented"
        static member Dense(length: int, initializer: int -> 'T, monoid: Monoid<'T>) : Vector<'T> =
            failwith "Not Implemented"
        static member ZeroDense(length: int, monoid: Monoid<'T>) : Vector<'T> =
            failwith "Not Implemented"

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =
    let toSeq (vector: Vector<'a>) = failwith "Not Implemented"
