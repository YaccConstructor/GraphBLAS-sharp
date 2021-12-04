namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

// TODO: переставить аргументы
module internal Standard =
    let runExcludeInplace
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (inputArray: ClArray<int>)
        (totalSum: ClArray<int>) =

        PrefixSum.runExcludeInplace
            clContext
            workGroupSize
            processor
            inputArray
            totalSum
            0
            <@ (+) @>
            0

