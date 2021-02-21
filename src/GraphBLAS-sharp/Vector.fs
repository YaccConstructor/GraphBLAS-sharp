namespace GraphBLAS.FSharp

[<AutoOpen>]
module VectorExtensions =
    type Vector<'a when 'a : struct and 'a : equality> with
        static member Sparse(denseVector: 'T[], zero: 'T) : Vector<'T> =
            failwith "Not Implemented"

        static member Sparse(length: int, values: (int * 'T) list) : Vector<'T> =
            failwith "Not Implemented"

        static member Sparse(length: int, initializer: int -> 'T) : Vector<'T> =
            failwith "Not Implemented"

        static member ZeroSparse(length: int) : Vector<'T> =
            upcast SparseVector(length, Array.zeroCreate<int> 0, Array.zeroCreate<'T> 0)

        static member Dense(denseVector: 'T[], monoid: Monoid<'T>) : Vector<'T> =
            upcast DenseVector(denseVector, monoid)

        static member Dense(length: int, initializer: int -> 'T, monoid: Monoid<'T>) : Vector<'T> =
            upcast DenseVector(Array.init length initializer, monoid)

        static member ZeroDense(length: int, monoid: Monoid<'T>) : Vector<'T> =
            upcast DenseVector(Array.create length monoid.Zero, monoid)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =
    let ofArray (denseVector: 'a[]) (zero: 'a) (vectorFormat : VectorBackendFormat) : Vector<'a> =
        failwith "Not Implemented"
