namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal Gather =
    /// <summary>
    /// Creates a new array obtained from positions replaced with values from the given array at these positions (indices).
    /// </summary>
    /// <example>
    /// <code>
    /// let ps = [| 2; 0; 2; 1 |]
    /// let arr = [| 1.4; 2.5; 3.6 |]
    /// let res = run clContext 64 processor ps arr
    /// ...
    /// > val res = [| 3.6; 1.4; 3.6; 2.5 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">.</param>
    ///<param name="processor">.</param>
    ///<param name="positions">Indices of the elements from input array.</param>
    ///<param name="inputArray">Values to gather.</param>
    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positions: ClArray<int>)
        (inputArray: ClArray<'a>) =

        let outputArray =
            clContext.CreateClArray(
                positions.Length,
                hostAccessMode = HostAccessMode.NotAccessible
            )

        let size = outputArray.Length

        let gather =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>)
                    (inputArray: ClArray<'a>)
                    (outputArray: ClArray<'a>) ->

                    let i = ndRange.GlobalID0

                    if i < size then outputArray.[i] <- inputArray.[positions.[i]]
            @>

        let kernel = clContext.CreateClKernel(gather)
        let ndRange = Range1D.CreateValid(size, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        positions
                        inputArray
                        outputArray)
        )
        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        outputArray
