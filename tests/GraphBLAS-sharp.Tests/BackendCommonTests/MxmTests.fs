module Backend.Mxm

open Expecto
open Expecto.Logging
open Expecto.Logging.Message

open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp

let logger = Log.create "Mxm.Tests"

let context = defaultContext.ClContext
let workGroupSize = 32

let makeTest context q zero isEqual plus mul mxmFun (leftMatrix: 'a [,], rightMatrix: 'a [,], mask: bool [,]) =

    let m1 =
        createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        createMatrixFromArray2D CSC rightMatrix (isEqual zero)

    if m1.NNZCount > 0 && m2.NNZCount > 0 then
        let expected =
            Array2D.init
            <| Array2D.length1 mask
            <| Array2D.length2 mask
            <| fun i j ->
                if mask.[i, j] then
                    (leftMatrix.[i, *], rightMatrix.[*, j])
                    ||> Array.map2 mul
                    |> Array.reduce plus
                else
                    zero

        let expected =
            createMatrixFromArray2D COO expected (isEqual zero)

        if expected.NNZCount > 0 then
            let m1 = m1.ToBackend context
            let m2 = m2.ToBackend context

            let mask =
                Mask2D.FromArray2D(mask, not).ToBackend context

            let result = mxmFun q m1 m2 mask
            let actual = Matrix.FromBackend q result

            m1.Dispose q
            m2.Dispose q
            mask.Dispose q
            result.Dispose q

            // Check result
            "Matrices should be equal"
            |> Expect.equal actual expected

let tests =

    let getCorrectnessTestName datatype = sprintf "Correctness on %s" datatype

    let config =
        { defaultConfig with
              arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSizeWithMask> ] }

    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ let add =
          <@ fun x y ->
              let mutable res = x + y

              if res = 0 then None else (Some res) @>

      let mult = <@ fun x y -> Some(x * y) @>

      let mxmFun =
          Matrix.mxm add mult context workGroupSize

      makeTest context q 0 (=) (+) (*) mxmFun
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let logicalOr =
          <@ fun x y ->
              let mutable res = None

              match x, y with
              | false, false -> res <- None
              | _ -> res <- Some true

              res @>

      let logicalAnd =
          <@ fun x y ->
              let mutable res = None

              match x, y with
              | true, true -> res <- Some true
              | _ -> res <- None

              res @>

      let mxmFun =
          Matrix.mxm logicalOr logicalAnd context workGroupSize

      makeTest context q false (=) (||) (&&) mxmFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]
    |> testList "Mxm tests"
