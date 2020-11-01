namespace GraphBLAS.FSharp

open Brahma.OpenCL

type OpenCLContext = {
    Provider: ComputeProvider
    CommandQueue: CommandQueue
}

type ComputationalContext<'a> = {
    OCLContext: OpenCLContext
    Semiring: Semiring<'a>
}
