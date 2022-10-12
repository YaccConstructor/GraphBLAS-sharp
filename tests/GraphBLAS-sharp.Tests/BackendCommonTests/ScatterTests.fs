module Backend.Scatter

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Scatter.Tests"

let context = defaultContext.ClContext

let makeTest (context: ClContext) (q: MailboxProcessor<_>) scatter (array: (int * 'a) []) (result: 'a []) =
    if array.Length > 0 then
        let expected = Array.copy result
        array
        |> Array.pairwise
        |> Array.iter
            (fun ((i, u), (j, _)) ->
                if i <> j && 0 <= i && i < expected.Length then expected.[i] <- u)
        let i, u = array.[array.Length - 1]
        if 0 <= i && i < expected.Length then expected.[i] <- u

        let positions, vals = Array.unzip array

        let actual =
            use clPositions = context.CreateClArray positions
            use clVals = context.CreateClArray vals
            use clResult = context.CreateClArray result

            scatter q clPositions clVals clResult

            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clResult, Array.zeroCreate result.Length, ch))

        (sprintf
            "Arrays should be equal. Actual is \n%A, expected \n%A"
            actual
            expected)
        |> compareArrays (=) actual expected

let testFixtures<'a when 'a: equality> config wgSize context q =
    let scatter: MailboxProcessor<_> -> ClArray<int> -> ClArray<'a> -> ClArray<'a> -> unit =
        Scatter.runInplace context wgSize

    makeTest context q scatter
    |> testPropertyWithConfig config (sprintf "Correctness on %A" typeof<'a>)

let tests =
    let context = defaultContext.ClContext
    let config = { defaultConfig with endSize = 1000000 }

    let wgSize = 32
    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures<int> config wgSize context q
      testFixtures<byte> config wgSize context q
      testFixtures<bool> config wgSize context q ]
    |> testList "Backend.Common.Scatter tests"
