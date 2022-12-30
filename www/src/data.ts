import { entries, fromEntries } from "next-utils/objs"

export type Region = 'NYC' | 'JC' | 'HOB'
export const Regions: Region[] = [ 'NYC', 'JC', 'HOB', ]
export const RegionQueryStrings: [Region, string][] = [ ['HOB','h'], ['NYC','n'], ['JC','j'], ]

export type UserType = 'Annual' | 'Daily'
export const UserTypes: UserType[] = ['Annual', 'Daily']
export const UserTypeQueryStrings: [UserType, string][] = [ ['Annual', 'a'], ['Daily','d'], ]

export type Gender = 'Male' | 'Female' | 'Unspecified'
export const Genders: Gender[] = ['Male' , 'Female' , 'Unspecified']
export const GenderQueryStrings: [ Gender, string ][] = [ ['Male', 'm'], ['Female', 'f'], ['Unspecified', 'u'], ]
export const Int2Gender: { [k: number]: Gender } = { 0: 'Unspecified', 1: 'Male', 2: 'Female' }
// Gender data became 100% "Unspecified" from February 2021; don't bother with per-entry
// rolling averages from that point onward
export const GenderRollingAvgCutoff = new Date('2021-02-01')

export type RideableType = 'Classic' | 'Docked' | 'Electric' | 'Unknown'
export const RideableTypes: RideableType[] = ['Classic', 'Docked' , 'Electric' , 'Unknown']
export const RideableTypeChars: [ RideableType, string ][] = [['Classic', 'c'], ['Docked','d'] , ['Electric','e'] , ['Unknown','u']]
export const NormalizeRideableType: { [k: string]: RideableType } = {
    'docked_bike': 'Docked',
    'classic_bike': 'Classic',
    'electric_bike': 'Electric',
    'unknown': 'Unknown',
    'motivate_dockless_bike': 'Unknown',
}

export type StackBy = 'None' | 'Region' | 'User Type' | 'Gender' | 'Rideable Type'
export const StackBys: [StackBy, string][] = [
    ['None', 'n'],
    ['Region', 'r'],
    ['Gender','g'],
    ['User Type','u'],
    ['Rideable Type','b'],
]

export const DEFAULT_COLORS = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']
export const RideableTypeColors: { [r in RideableType]: string } = {
    'Unknown': '#636EFA',
    'Docked': '#EF553B',
    'Electric': '#AB63FA',
    'Classic': '#00CC96',
}
export const GenderColors: { [g in Gender]: string } = {
    'Unspecified': '#AB63FA',
    'Male': '#19D3F3',
    'Female': '#FFA15A',
}
export const UserTypeColors: { [u in UserType]: string } = { 'Daily': '#FF6692', 'Annual': '#FF97ff', }
export const RegionColors: { [r in Region]: string } = { 'NYC': '#636efa', 'HOB': '#63aefa', 'JC': '#632bfa', }
export const Colors = {
    None: { '': DEFAULT_COLORS[0], Total: 'black', },
    Region: RegionColors,
    Gender: GenderColors,
    'User Type': UserTypeColors,
    'Rideable Type': RideableTypeColors,
}

export type YAxis = 'Rides' | 'Ride minutes'
export const YAxes: [YAxis, string][] = [ ['Rides', 'r'], ['Ride minutes', 'm'], ]
export const yAxisLabelDict: { [k in YAxis]: { yAxis: string, title: string, hoverLabel: string } } = {
    'Rides': { yAxis: 'Total Rides', title: 'Citibike Rides per Month', hoverLabel: 'Rides' },
    'Ride minutes': { yAxis: 'Total Ride Minutes', title: 'Citibike Ride Minutes per Month', hoverLabel: 'Minutes', },
}

export type Row = {
    Year: number
    Month: number
    Count: number
    Duration: number
    Region: Region
    'User Type': UserType
    Gender: number
    'Rideable Type': string
}

export const stackKeyDict = {
    'None': [''],
    'User Type': ['Daily', 'Annual'],
    'Gender': ['Unspecified', 'Male', 'Female'],
    'Rideable Type': ['Docked', 'Electric', 'Unknown'],
    'Region': [ 'JC', 'HOB', 'NYC', ],
}

type Series<K extends string = string> = { [k in K]: number }
export function rollingAvg<K extends string = string>(vs: Series<K>, n: number, ): Series<K> {
    let avgs: [ K, number, ][] = []
    let sum: number = 0
    let elems: number[] = [];
    (entries<number>(vs) as [ K, number ][]).forEach(([ k, v ], i) => {
        sum += v
        elems.push(v)
        if (i >= n) {
            const evicted: number = elems.shift() || NaN
            sum -= evicted
            const avg = sum / n
            avgs.push([ k, avg, ])
        } else {
            avgs.push([ k, NaN, ])
        }
    })
    return fromEntries(avgs) as Series<K>
}
