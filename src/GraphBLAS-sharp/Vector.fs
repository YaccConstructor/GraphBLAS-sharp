namespace GraphBLAS.FSharp

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Vector =
    type Vector<'a when 'a : struct and 'a : equality> with
        static member Dense(monoid, ?array) =
            let array = defaultArg array (Array.zeroCreate<'a> 0)
            DenseVector(array, monoid)
