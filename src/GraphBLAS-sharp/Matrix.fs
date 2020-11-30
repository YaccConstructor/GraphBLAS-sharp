namespace GraphBLAS.FSharp

[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Matrix =
    type Matrix<'a when 'a : struct and 'a : equality> with
        static member Build (filename: string) : Matrix<'b> = failwith "Not Implemented"
