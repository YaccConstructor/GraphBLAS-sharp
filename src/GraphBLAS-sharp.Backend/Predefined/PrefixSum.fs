namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
module internal PrefixSum =
    let standardExcludeInplace (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumExcludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor inputArray totalSum 0

    let standardIncludeInplace (clContext: ClContext) workGroupSize =

        let scan =
            ClArray.prefixSumIncludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor inputArray totalSum 0

    let standardInclude (clContext: ClContext) workGroupSize flag =

        let scan =
            ClArray.prefixSumInclude <@ (+) @> clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor inputArray totalSum 0

    let standardExclude (clContext: ClContext) workGroupSize flag =

        let scan =
            ClArray.prefixSumExclude <@ (+) @> clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) (totalSum: ClCell<int>) ->

            scan processor inputArray totalSum 0
