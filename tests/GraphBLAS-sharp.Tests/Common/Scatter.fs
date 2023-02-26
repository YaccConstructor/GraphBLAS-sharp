module GraphBLAS.FSharp.Tests.Backend.Common.Scatter

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "Scatter.Tests"

let context = defaultContext.ClContext

let config =
    { Tests.Utils.defaultConfig with
          endSize = 1000000 }

let wgSize = Tests.Utils.defaultWorkGroupSize

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

        let positions, values = Array.unzip array

        let actual =
            use clPositions = context.CreateClArray positions
            use clValues = context.CreateClArray values
            use clResult = context.CreateClArray result

            scatter q clPositions clValues clResult

            q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clResult, Array.zeroCreate result.Length, ch))

        $"Arrays should be equal. Actual is \n%A{actual}, expected \n%A{expected}"
        |> Tests.Utils.compareArrays (=) actual expected

let testFixtures<'a when 'a: equality> =
    Scatter.runInplace<'a> context wgSize
    |> makeTest
    |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>}"

let tests =
    q.Error.Add(fun e -> failwithf $"%A{e}")

    [ testFixtures<int>
      testFixtures<byte>
      testFixtures<bool> ]
    |> testList "Backend.Common.Scatter tests"
