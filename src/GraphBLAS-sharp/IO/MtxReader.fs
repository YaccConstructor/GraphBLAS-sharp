namespace GraphBLAS.FSharp.IO

// open System.IO
// open GraphBLAS.FSharp
// open System

// type MtxShape =
//     {
//         Filename: string
//         Object: string
//         Format: string
//         Field: string
//         Symmetry: string
//         Size: int[]
//     }

//     member this.RowCount = this.Size.[0]
//     member this.ColumnCount = this.Size.[1]

//     override this.ToString() =
//         sprintf "%s" <| Path.GetFileNameWithoutExtension this.Filename

// module MtxReader =
//     let private readShapeWithReader (streamReader: StreamReader) (pathToFile: string) =
//         let shape = streamReader.ReadLine().Split(' ')
//         let object = shape.[1]
//         let format = shape.[2]
//         let field = shape.[3]
//         let symmetry = shape.[4]

//         while streamReader.Peek() = int '%' do
//             streamReader.ReadLine() |> ignore

//         let size =
//             streamReader.ReadLine().Split(' ')
//             |> Array.map int

//         {
//             Filename = pathToFile |> Path.GetFileName
//             Object = object
//             Format = format
//             Field = field
//             Symmetry = symmetry
//             Size = size
//         }

//     let readShapeFromFile (pathToFile: string) =
//         use streamReader = new StreamReader(pathToFile)
//         readShapeWithReader streamReader pathToFile

//     let private readGenericMatrixFromFile (pathToFile: string) : Matrix<'a> =
//         use streamReader = new StreamReader(pathToFile)
//         let shape = readShapeWithReader streamReader pathToFile

//         let len =
//             match shape.Format with
//             | "array" -> shape.Size.[0] * shape.Size.[1]
//             | "coordinate" -> shape.Size.[2]
//             | _ -> failwith "Unsupported matrix format"

//         let data =
//             [0 .. len - 1]
//             |> List.map (fun _ -> streamReader.ReadLine().Split(' '))

//         let makeCOO () =
//             let pack x y = (uint64 x <<< 32) ||| (uint64 y)
//             let unpack x = (int ((x &&& 0xFFFFFFFF0000000UL) >>> 32)), (int (x &&& 0xFFFFFFFUL))

//             data
//             |> Array.ofList
//             |> Array.Parallel.map
//                 (fun line ->
//                     let value = Convert.ChangeType(line.[2], typeof<'a>) |> unbox<'a>
//                     struct(pack <| int line.[0] <| int line.[1], value)
//                 )
//             |> Array.sortBy (fun struct(packedIndex, _) -> packedIndex)
//             |>
//                 fun data ->
//                     let rows = Array.zeroCreate data.Length
//                     let cols = Array.zeroCreate data.Length
//                     let values = Array.zeroCreate data.Length

//                     Array.Parallel.iteri (fun i struct(packedIndex, value) ->
//                         let (rowIdx, columnIdx) = unpack packedIndex
//                         // in mtx indecies start at 1
//                         rows.[i] <- rowIdx - 1
//                         cols.[i] <- columnIdx - 1
//                         values.[i] <- value
//                     ) data

//                     {
//                         Rows = rows
//                         Columns = cols
//                         Values = values
//                         RowCount = shape.RowCount
//                         ColumnCount = shape.ColumnCount
//                     }


//         match shape.Format with
//         | "array" -> failwith "Unsupported matrix format"
//         | "coordinate" -> MatrixCOO <| makeCOO ()

//     let readRealMatrix (pathToFile: string) : Matrix<float32> =
//         readGenericMatrixFromFile pathToFile
