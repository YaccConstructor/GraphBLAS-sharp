module GraphBLAS.FSharp.Tests.Backend.Common.Scatter

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Scatter.Tests"

let context = defaultContext.ClContext
let config = { defaultConfig with endSize = 1000000 }

let wgSize = 32

let q = defaultContext.Queue

let makeTest scatter (array: (int * 'a) []) (result: 'a []) =
    if array.Length > 0 then
        let expected = Array.copy result

        array
        |> Array.pairwise
        |> Array.iter
            (fun ((i, u), (j, _)) ->
                if i <> j && 0 <= i && i < expected.Length then
                    expected.[i] <- u)

        let i, u = array.[array.Length - 1]

        if 0 <= i && i < expected.Length then
            expected.[i] <- u

        let positions, vals = Array.unzip array

        let actual =
            use clPositions = context.CreateClArray positions
            use clVals = context.CreateClArray vals
            use clResult = context.CreateClArray result

            scatter q clPositions clVals clResult

            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clResult, Array.zeroCreate result.Length, ch))

        (sprintf "Arrays should be equal. Actual is \n%A, expected \n%A" actual expected)
        |> compareArrays (=) actual expected

let testFixtures<'a when 'a: equality> =
    let scatter = Scatter.runInplace<'a> context wgSize

    makeTest scatter
    |> testPropertyWithConfig config (sprintf "Correctness on %A" typeof<'a>)

let tests =
    q.Error.Add(fun e -> failwithf "%A" e)

    [ testFixtures<int>
      testFixtures<byte>
      testFixtures<bool> ]
    |> testList "Backend.Common.Scatter tests"
