namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal Gather =
    /// <summary>
    /// Creates a new array obtained from positions replaced with values from the given array at these positions (indices).
    /// </summary>
    /// <example>
    /// <code>
    /// let positions = [| 2; 0; 2; 1 |]
    /// let array = [| 1.4; 2.5; 3.6 |]
    /// ...
    /// > val result = [| 3.6; 1.4; 3.6; 2.5 |]
    /// </code>
    /// </example>
    let run (clContext: ClContext) workGroupSize =

        let gather =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (inputArray: ClArray<'a>) (outputArray: ClArray<'a>) (size: int) ->

                let i = ndRange.GlobalID0

                if i < size then
                    outputArray.[i] <- inputArray.[positions.[i]] @>

        let program = clContext.Compile(gather)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (inputArray: ClArray<'a>) (outputArray: ClArray<'a>) ->

            let size = outputArray.Length

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(size, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange positions inputArray outputArray size)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
