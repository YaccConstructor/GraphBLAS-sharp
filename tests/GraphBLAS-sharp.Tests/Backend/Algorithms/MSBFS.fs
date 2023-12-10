module GraphBLAS.FSharp.Tests.Backend.Algorithms.MSBFS

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.CreateGraph
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let config = Utils.undirectedAlgoConfig

let workGroupSize = Utils.defaultWorkGroupSize

let makeLevelsTest context queue bfs (matrix: int [,]) =
    let graph = undirectedFromArray2D matrix 0

    let largestComponent =
        ConnectedComponents.largestComponent graph

    Array.sortInPlace largestComponent

    if largestComponent.Length > 1 then
        let sourceVertexCount = max 2 (largestComponent.Length / 10)

        let source =
            largestComponent.[0..sourceVertexCount - 1]
            |> Array.sort
            |> Array.toList

        let matrixHost =
            Utils.createMatrixFromArray2D CSR matrix ((=) 0)

        let matrixDevice = matrixHost.ToDevice context

        let expectedArray2D: int [,] =
            Array2D.zeroCreate sourceVertexCount (Array2D.length1 matrix)

        source
        |> List.iteri
            (fun i vertex ->
                (snd (BFS.runUndirected graph vertex))
                |> Utils.createArrayFromDictionary (Array2D.length1 matrix) 0
                |> Array.iteri (fun col value -> expectedArray2D.[i, col] <- value))

        let expected =
            Utils.createMatrixFromArray2D COO expectedArray2D ((=) 0)

        let actual: ClMatrix<int> = bfs queue matrixDevice source
        let actual = actual.ToHostAndFree queue

        matrixDevice.Dispose queue

        match actual, expected with
        | Matrix.COO a, Matrix.COO e -> Utils.compareCOOMatrix (=) a e
        | _ -> failwith "Not implemented"

let createLevelsTest<'a> context queue testFun =
    testFun
    |> makeLevelsTest context queue
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}, %A{context}"

let levelsTestFixtures (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue

      let bfsLevels =
          Algorithms.MSBFS.runLevels
              ArithmeticOperations.intAddWithoutZero
              ArithmeticOperations.intMulWithoutZero
              context
              workGroupSize

      createLevelsTest<int> context queue bfsLevels ]

let levelsTests =
    TestCases.gpuTests "MSBFS Levels tests" levelsTestFixtures

let makeParentsTest context queue bfs (matrix: int [,]) =

    let graph = undirectedFromArray2D matrix 0

    let largestComponent =
        ConnectedComponents.largestComponent graph

    if largestComponent.Length > 1 then
        let sourceVertexCount = max 2 (largestComponent.Length / 10)

        let source = largestComponent.[0..sourceVertexCount]
        source |> Array.sortInPlace
        let source = source |> Array.toList

        let matrixHost =
            Utils.createMatrixFromArray2D CSR matrix ((=) 0)

        let matrixDevice = matrixHost.ToDevice context

        let expected =
            HostPrimitives.MSBFSParents matrix source

        let actual: ClMatrix<int> = bfs queue matrixDevice source
        let actual = actual.ToHostAndFree queue

        matrixDevice.Dispose queue

        match actual, expected with
        | Matrix.COO a, Matrix.COO e -> Utils.compareCOOMatrix (=) a e
        | _ -> failwith "Not implemented"

let createParentsTest<'a> context queue testFun =
    testFun
    |> makeParentsTest context queue
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}, %A{context}"

let parentsTestFixtures (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue

      let bfsParents =
          Algorithms.MSBFS.runParents context workGroupSize

      createParentsTest context queue bfsParents ]

let parentsTests =
    TestCases.gpuTests "MSBFS Parents tests" parentsTestFixtures
