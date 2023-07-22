module GraphBLAS.FSharp.Tests.Backend.Matrix.SpGeMM.Masked

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context

let logger = Log.create "SpGeMM.Masked.Tests"

let context = defaultContext.ClContext
let workGroupSize = Utils.defaultWorkGroupSize

let makeTest context q zero isEqual plus mul mxmFun (leftMatrix: 'a [,], rightMatrix: 'a [,], mask: bool [,]) =

    let m1 =
        Utils.createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        Utils.createMatrixFromArray2D CSC rightMatrix (isEqual zero)

    let matrixMask =
        Utils.createMatrixFromArray2D COO mask ((=) false)

    if m1.NNZ > 0 && m2.NNZ > 0 then
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
            Utils.createMatrixFromArray2D COO expected (isEqual zero)

        if expected.NNZ > 0 then
            let m1 = m1.ToDevice context
            let m2 = m2.ToDevice context
            let matrixMask = matrixMask.ToDevice context

            let (result: ClMatrix<'a>) = mxmFun q m1 m2 matrixMask
            let actual = result.ToHost q

            m1.Dispose q
            m2.Dispose q
            matrixMask.Dispose q
            result.Dispose q

            // Check result
            "Matrices should be equal"
            |> Expect.equal actual expected

let tests =
    let getCorrectnessTestName = sprintf "Correctness on %s"

    let config =
        { Utils.defaultConfig with
              arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSizeWithMask> ] }

    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ let add =
          <@ fun x y ->
              let mutable res = x + y

              if res = 0 then None else (Some res) @>

      let mult = <@ fun x y -> Some(x * y) @>

      let mxmFun =
          Operations.SpGeMM.masked add mult context workGroupSize

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
          Operations.SpGeMM.masked logicalOr logicalAnd context workGroupSize

      makeTest context q false (=) (||) (&&) mxmFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]
    |> testList "SpGeMM masked tests"
