namespace GraphBLAS.FSharp

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =
    let build (size: int) (indices: int[]) (values: int[]) : Vector<'a> =
        failwith "Not Implemented yet"

    // ambiguous name (tuples = коллекция троек или 3 коллекции)
    let ofTuples (size: int) (elements: (int * 'a) list) : Vector<'a> =
        failwith "Not Implemented yet"

    let ofArray (array: 'a[]) (isZero: 'a -> bool) : Vector<'a> =
        failwith "Not Implemented yet"

    let init (size: int) (initializer: int -> 'a) : Vector<'a> =
        failwith "Not Implemented yet"

    let zeroCreate (size: int) : Vector<'a> =
        failwith "Not Implemented yet"
