# Home Venue Detection for Team ELO
#
# Functions for detecting home team advantage in cricket matches.
# Dual approach:
#   - Club teams: Match venue to team's most-played venue (mode)
#   - International: Match venue to country using hard-coded lookup


#' Build Home Venue Lookups
#'
#' Builds lookup tables for detecting home teams in matches.
#' Returns two lookups:
#' - club_home: Maps team_id to their home venue (mode venue)
#' - venue_country: Maps venue names to country names
#'
#' @param all_matches Data frame of matches with columns: team_type, team1, team2, venue, gender, match_type
#' @param format The format to use for team IDs: "t20", "odi", or "test".
#'   If NULL (default), derives format from match_type column.
#'
#' @return List with club_home (named character vector) and venue_country (named character vector)
#' @keywords internal
build_home_lookups <- function(all_matches, format = NULL) {

  # Format groupings for deriving format from match_type
  format_groups <- list(
    t20 = c("T20", "IT20"),
    odi = c("ODI", "ODM"),
    test = c("Test", "MDM")
  )

  # --- CLUB: team_id -> home_venue (mode venue) ---
  club_matches <- all_matches %>% dplyr::filter(.data$team_type == "club")

  if (nrow(club_matches) > 0) {
    club_home_venues <- club_matches %>%
      tidyr::pivot_longer(cols = c("team1", "team2"), names_to = "role", values_to = "team")

    # Derive format from match_type if not specified
    if (is.null(format)) {
      club_home_venues <- club_home_venues %>%
        dplyr::mutate(
          format = dplyr::case_when(
            .data$match_type %in% format_groups$t20 ~ "t20",
            .data$match_type %in% format_groups$odi ~ "odi",
            .data$match_type %in% format_groups$test ~ "test",
            TRUE ~ tolower(.data$match_type)
          ),
          team_id = make_team_id_vec(.data$team, .data$gender, .data$format, .data$team_type)
        )
    } else {
      club_home_venues <- club_home_venues %>%
        dplyr::mutate(team_id = make_team_id_vec(.data$team, .data$gender, format, .data$team_type))
    }

    club_home_venues <- club_home_venues %>%
      dplyr::filter(!is.na(.data$venue), .data$venue != "") %>%
      dplyr::group_by(.data$team_id, .data$venue) %>%
      dplyr::summarise(n_matches = dplyr::n(), .groups = "drop") %>%
      dplyr::group_by(.data$team_id) %>%
      dplyr::slice_max(.data$n_matches, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup()

    club_home_lookup <- stats::setNames(club_home_venues$venue, club_home_venues$team_id)
  } else {
    club_home_lookup <- character(0)
  }

  # --- INTERNATIONAL: venue -> country ---
  venue_country_lookup <- build_venue_country_lookup(all_matches)

  list(
    club_home = club_home_lookup,
    venue_country = venue_country_lookup
  )
}


#' Detect Home Team
#'
#' Determines which team (if any) is playing at home.
#' Uses dual lookup approach:
#' - Club matches: Check if venue matches team's home ground
#' - International: Check if venue is in team's country
#'
#' @param team1 Name of team 1
#' @param team2 Name of team 2
#' @param team1_id Composite ID of team 1
#' @param team2_id Composite ID of team 2
#' @param venue Venue name
#' @param team_type Either "club" or "international"
#' @param club_home_lookup Named vector mapping team_id to home venue
#' @param venue_country_lookup Named vector mapping venue to country
#'
#' @return Integer: 1 if team1 is home, -1 if team2 is home, 0 if neutral/unknown
#' @keywords internal
detect_home_team <- function(team1, team2, team1_id, team2_id, venue, team_type,
                             club_home_lookup, venue_country_lookup) {
  if (is.na(venue) || venue == "") return(0L)

  if (team_type == "club") {
    # Club: Check if venue matches either team's home ground
    # Use safe lookup with match() to avoid subscript out of bounds
    t1_idx <- match(team1_id, names(club_home_lookup))
    t2_idx <- match(team2_id, names(club_home_lookup))

    t1_home <- if (!is.na(t1_idx)) club_home_lookup[t1_idx] else NA_character_
    t2_home <- if (!is.na(t2_idx)) club_home_lookup[t2_idx] else NA_character_

    if (!is.na(t1_home) && venue == t1_home) return(1L)
    if (!is.na(t2_home) && venue == t2_home) return(-1L)

  } else {
    # International: Check which country the venue is in
    venue_idx <- match(venue, names(venue_country_lookup))
    venue_country <- if (!is.na(venue_idx)) venue_country_lookup[venue_idx] else NA_character_

    if (!is.na(venue_country)) {
      # Match country to team name (exact match)
      if (venue_country == team1) return(1L)
      if (venue_country == team2) return(-1L)
    }
  }

  return(0L)  # Neutral
}


#' Get Venue to Country Mapping
#'
#' Returns a named list mapping international cricket venue names to country names.
#' Country names match team names as they appear in the data (e.g., "West Indies" not "Barbados").
#'
#' @return Named list where names are venue names and values are country names
#' @keywords internal
get_venue_country_map <- function() {
  list(
    # ===========================================
    # INDIA
    # ===========================================
    "Wankhede Stadium" = "India",
    "M.A.Chidambaram Stadium" = "India",
    "M.A. Chidambaram Stadium" = "India",
    "Eden Gardens" = "India",
    "Narendra Modi Stadium" = "India",
    "M.Chinnaswamy Stadium" = "India",
    "M. Chinnaswamy Stadium" = "India",
    "Rajiv Gandhi International Stadium" = "India",
    "Rajiv Gandhi International Cricket Stadium" = "India",
    "Arun Jaitley Stadium" = "India",
    "Feroz Shah Kotla" = "India",
    "Punjab Cricket Association IS Bindra Stadium" = "India",
    "Punjab Cricket Association Stadium" = "India",
    "IS Bindra Stadium" = "India",
    "Himachal Pradesh Cricket Association Stadium" = "India",
    "HPCA Stadium" = "India",
    "Bharat Ratna Shri Atal Bihari Vajpayee Ekana Cricket Stadium" = "India",
    "Ekana Cricket Stadium" = "India",
    "EKANA Stadium" = "India",
    "Barsapara Cricket Stadium" = "India",
    "ACA-VDCA Stadium" = "India",
    "Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium" = "India",
    "Greenfield International Stadium" = "India",
    "Greenfield Stadium" = "India",
    "Sawai Mansingh Stadium" = "India",
    "Holkar Cricket Stadium" = "India",
    "Maharashtra Cricket Association Stadium" = "India",
    "MCA Stadium" = "India",
    "Subrata Roy Sahara Stadium" = "India",
    "Brabourne Stadium" = "India",
    "DY Patil Stadium" = "India",
    "D.Y. Patil Stadium" = "India",
    "Nehru Stadium" = "India",
    "Nehru Stadium, Kochi" = "India",
    "Nehru Stadium, Indore" = "India",
    "Jawaharlal Nehru Stadium" = "India",
    "Barabati Stadium" = "India",
    "Green Park" = "India",
    "JSCA International Stadium Complex" = "India",
    "JSCA Stadium" = "India",
    "Vidarbha Cricket Association Stadium" = "India",
    "VCA Stadium" = "India",
    "Saurashtra Cricket Association Stadium" = "India",
    "Saurashtra Stadium" = "India",
    "Lalabhai Contractor Stadium" = "India",
    "Dr DY Patil Sports Academy" = "India",
    "Sardar Patel Stadium" = "India",
    "Motera Stadium" = "India",

    # ===========================================
    # AUSTRALIA
    # ===========================================
    "Melbourne Cricket Ground" = "Australia",
    "MCG" = "Australia",
    "Sydney Cricket Ground" = "Australia",
    "SCG" = "Australia",
    "Brisbane Cricket Ground, Woolloongabba" = "Australia",
    "The Gabba" = "Australia",
    "Gabba" = "Australia",
    "Adelaide Oval" = "Australia",
    "Optus Stadium" = "Australia",
    "Perth Stadium" = "Australia",
    "WACA Ground" = "Australia",
    "W.A.C.A. Ground" = "Australia",
    "Bellerive Oval" = "Australia",
    "Blundstone Arena" = "Australia",
    "Manuka Oval" = "Australia",
    "Carrara Oval" = "Australia",
    "Metricon Stadium" = "Australia",
    "Junction Oval" = "Australia",
    "Cazaly's Stadium" = "Australia",
    "Marrara Cricket Ground" = "Australia",
    "TIO Stadium" = "Australia",
    "Aurora Stadium" = "Australia",
    "North Sydney Oval" = "Australia",
    "Allan Border Field" = "Australia",
    "GMHBA Stadium" = "Australia",
    "Kardinia Park" = "Australia",
    "Simonds Stadium" = "Australia",
    "Mackay Great Barrier Reef Arena" = "Australia",
    "Tony Ireland Stadium" = "Australia",
    "Showground Stadium" = "Australia",
    "Sydney Showground Stadium" = "Australia",
    "Blacktown International Sportspark" = "Australia",
    "Hurstville Oval" = "Australia",
    "Karen Rolton Oval" = "Australia",
    "CitiPower Centre" = "Australia",
    "Casey Fields" = "Australia",
    "Eastern Oval" = "Australia",
    "Coffs Harbour International Stadium" = "Australia",
    "Drummoyne Oval" = "Australia",

    # ===========================================
    # ENGLAND
    # ===========================================
    "Lord's" = "England",
    "The Oval" = "England",
    "Kennington Oval" = "England",
    "Edgbaston" = "England",
    "Old Trafford" = "England",
    "Emirates Old Trafford" = "England",
    "Headingley" = "England",
    "Trent Bridge" = "England",
    "The Rose Bowl" = "England",
    "Rose Bowl" = "England",
    "Ageas Bowl" = "England",
    "Sophia Gardens" = "England",
    "SWALEC Stadium" = "England",
    "Riverside Ground" = "England",
    "Emirates Riverside" = "England",
    "Seat Unique Riverside" = "England",
    "County Ground, Bristol" = "England",
    "Bristol County Ground" = "England",
    "County Ground, Taunton" = "England",
    "Cooper Associates County Ground" = "England",
    "County Ground, Chelmsford" = "England",
    "County Ground, Northampton" = "England",
    "County Ground, Derby" = "England",
    "The 1st Central County Ground" = "England",
    "1st Central County Ground" = "England",
    "County Ground, Hove" = "England",
    "New Road" = "England",
    "Blackfinch New Road" = "England",
    "Worcestershire County Ground" = "England",
    "St Lawrence Ground" = "England",
    "The Spitfire Ground, St Lawrence" = "England",
    "The Uptonsteel County Ground" = "England",
    "Utilita Bowl" = "England",
    "Wormsley Cricket Ground" = "England",
    "Wormsley" = "England",
    "Arundel Castle Cricket Club Ground" = "England",
    "Haslegrave Ground" = "England",
    "Radlett" = "England",
    "Merchant Taylors School" = "England",
    "Scarborough" = "England",
    "Chester-le-Street" = "England",
    "Loughborough" = "England",
    "Loughborough University Ground" = "England",
    "Vineyard Cricket Ground" = "England",

    # ===========================================
    # SOUTH AFRICA
    # ===========================================
    "The Wanderers Stadium" = "South Africa",
    "Wanderers Stadium" = "South Africa",
    "Newlands" = "South Africa",
    "Kingsmead" = "South Africa",
    "SuperSport Park" = "South Africa",
    "Centurion" = "South Africa",
    "St George's Park" = "South Africa",
    "Boland Park" = "South Africa",
    "Mangaung Oval" = "South Africa",
    "Senwes Park" = "South Africa",
    "Buffalo Park" = "South Africa",
    "De Beers Diamond Oval" = "South Africa",
    "Diamond Oval" = "South Africa",
    "OUTsurance Oval" = "South Africa",
    "Outsurance Oval" = "South Africa",
    "Willowmoore Park" = "South Africa",
    "L.C. de Villiers Oval" = "South Africa",
    "LC de Villiers Oval" = "South Africa",
    "City Oval" = "South Africa",
    "Moses Mabhida Stadium" = "South Africa",
    "Hollywoodbets Kingsmead Stadium" = "South Africa",
    "Eurolux Boland Park" = "South Africa",
    "Six Gun Grill Newlands" = "South Africa",
    "Imperial Wanderers Stadium" = "South Africa",
    "Bidvest Wanderers Stadium" = "South Africa",

    # ===========================================
    # NEW ZEALAND
    # ===========================================
    "Eden Park" = "New Zealand",
    "Basin Reserve" = "New Zealand",
    "Hagley Oval" = "New Zealand",
    "Seddon Park" = "New Zealand",
    "McLean Park" = "New Zealand",
    "Bay Oval" = "New Zealand",
    "Mount Maunganui" = "New Zealand",
    "Saxton Oval" = "New Zealand",
    "University Oval" = "New Zealand",
    "Pukekura Park" = "New Zealand",
    "John Davies Oval" = "New Zealand",
    "Sky Stadium" = "New Zealand",
    "Westpac Stadium" = "New Zealand",
    "AMI Stadium" = "New Zealand",
    "Jade Stadium" = "New Zealand",
    "Lancaster Park" = "New Zealand",
    "Queenstown Events Centre" = "New Zealand",
    "Cobham Oval" = "New Zealand",
    "Bert Sutcliffe Oval" = "New Zealand",
    "Lincoln Green" = "New Zealand",

    # ===========================================
    # PAKISTAN
    # ===========================================
    "National Stadium" = "Pakistan",
    "National Stadium, Karachi" = "Pakistan",
    "Gaddafi Stadium" = "Pakistan",
    "Rawalpindi Cricket Stadium" = "Pakistan",
    "Pindi Cricket Stadium" = "Pakistan",
    "Multan Cricket Stadium" = "Pakistan",
    "Iqbal Stadium" = "Pakistan",
    "Faisalabad" = "Pakistan",
    "Arbab Niaz Stadium" = "Pakistan",
    "Peshawar" = "Pakistan",
    "Southend Club" = "Pakistan",

    # ===========================================
    # SRI LANKA
    # ===========================================
    "R.Premadasa Stadium" = "Sri Lanka",
    "R. Premadasa Stadium" = "Sri Lanka",
    "Rangiri Dambulla International Stadium" = "Sri Lanka",
    "Rangiri Dambulla" = "Sri Lanka",
    "Pallekele International Cricket Stadium" = "Sri Lanka",
    "Pallekele" = "Sri Lanka",
    "Sinhalese Sports Club Ground" = "Sri Lanka",
    "SSC" = "Sri Lanka",
    "P Sara Oval" = "Sri Lanka",
    "P. Sara Oval" = "Sri Lanka",
    "Galle International Stadium" = "Sri Lanka",
    "Mahinda Rajapaksa International Cricket Stadium" = "Sri Lanka",
    "Sooriyawewa Stadium" = "Sri Lanka",
    "Hambantota" = "Sri Lanka",
    "Khettarama Stadium" = "Sri Lanka",
    "Colombo Cricket Club Ground" = "Sri Lanka",
    "Nondescripts Cricket Club Ground" = "Sri Lanka",
    "Colts Cricket Club Ground" = "Sri Lanka",
    "Chilaw Marians" = "Sri Lanka",
    "Asgiriya Stadium" = "Sri Lanka",
    "Mercantile Cricket Association Ground" = "Sri Lanka",

    # ===========================================
    # WEST INDIES
    # ===========================================
    "Kensington Oval" = "West Indies",
    "Sabina Park" = "West Indies",
    "Queen's Park Oval" = "West Indies",
    "Queens Park Oval" = "West Indies",
    "Brian Lara Stadium" = "West Indies",
    "Brian Lara Cricket Academy" = "West Indies",
    "Sir Vivian Richards Stadium" = "West Indies",
    "Coolidge Cricket Ground" = "West Indies",
    "Warner Park" = "West Indies",
    "Providence Stadium" = "West Indies",
    "Guyana National Stadium" = "West Indies",
    "Daren Sammy National Cricket Stadium" = "West Indies",
    "Darren Sammy National Cricket Stadium" = "West Indies",
    "Beausejour Stadium" = "West Indies",
    "Windsor Park" = "West Indies",
    "Arnos Vale Ground" = "West Indies",
    "Three Ws Oval" = "West Indies",
    "3Ws Oval" = "West Indies",
    "Cave Hill" = "West Indies",
    "National Cricket Stadium, Grenada" = "West Indies",
    "Albion Sports Complex" = "West Indies",
    "Central Broward Stadium" = "West Indies",

    # ===========================================
    # BANGLADESH
    # ===========================================
    "Shere Bangla National Stadium" = "Bangladesh",
    "Sher-e-Bangla National Cricket Stadium" = "Bangladesh",
    "Zahur Ahmed Chowdhury Stadium" = "Bangladesh",
    "Sylhet International Cricket Stadium" = "Bangladesh",
    "Sheikh Abu Naser Stadium" = "Bangladesh",
    "Sheikh Kamal International Cricket Stadium" = "Bangladesh",
    "Khan Shaheb Osman Ali Stadium" = "Bangladesh",
    "Mirpur" = "Bangladesh",
    "Chattogram" = "Bangladesh",
    "Chittagong" = "Bangladesh",

    # ===========================================
    # ZIMBABWE
    # ===========================================
    "Harare Sports Club" = "Zimbabwe",
    "Queens Sports Club" = "Zimbabwe",
    "Takashinga Cricket Club" = "Zimbabwe",
    "Old Hararians" = "Zimbabwe",
    "Bulawayo Athletic Club" = "Zimbabwe",

    # ===========================================
    # IRELAND
    # ===========================================
    "Malahide" = "Ireland",
    "The Village" = "Ireland",
    "The Village, Dublin" = "Ireland",
    "Castle Avenue" = "Ireland",
    "Castle Avenue, Dublin" = "Ireland",
    "Clontarf Cricket Club Ground" = "Ireland",
    "Stormont" = "Ireland",
    "Civil Service Cricket Club" = "Ireland",
    "Bready Cricket Club" = "Ireland",
    "Bready" = "Ireland",
    "Sydney Parade" = "Ireland",
    "Pembroke Cricket Club Ground" = "Ireland",
    "Merrion Cricket Club Ground" = "Ireland",
    "YMCA Cricket Club Ground" = "Ireland",
    "Oak Hill Cricket Ground" = "Ireland",

    # ===========================================
    # UNITED ARAB EMIRATES
    # ===========================================
    "Dubai International Cricket Stadium" = "United Arab Emirates",
    "Dubai Sports City" = "United Arab Emirates",
    "Dubai" = "United Arab Emirates",
    "Sheikh Zayed Stadium" = "United Arab Emirates",
    "Sheikh Zayed Cricket Stadium" = "United Arab Emirates",
    "Abu Dhabi" = "United Arab Emirates",
    "Sharjah Cricket Stadium" = "United Arab Emirates",
    "Sharjah" = "United Arab Emirates",
    "Zayed Cricket Stadium" = "United Arab Emirates",
    "ICC Academy" = "United Arab Emirates",
    "ICC Academy Ground" = "United Arab Emirates",
    "ICC Global Cricket Academy" = "United Arab Emirates",
    "Tolerance Oval" = "United Arab Emirates",
    "ICC Academy Oval 2" = "United Arab Emirates",
    "Al Dhaid Cricket Ground" = "United Arab Emirates",
    "Ajman Oval" = "United Arab Emirates",

    # ===========================================
    # SCOTLAND
    # ===========================================
    "The Grange Club" = "Scotland",
    "Grange Cricket Club Ground" = "Scotland",
    "Titwood" = "Scotland",
    "Myreside" = "Scotland",
    "Goldenacre" = "Scotland",
    "Forthill" = "Scotland",
    "New Williamfield" = "Scotland",
    "Mannofield Park" = "Scotland",
    "Cambusdoon New Ground" = "Scotland",
    "Lochlands" = "Scotland",

    # ===========================================
    # NETHERLANDS
    # ===========================================
    "VRA Ground" = "Netherlands",
    "Amstelveen" = "Netherlands",
    "Sportpark Westvliet" = "Netherlands",
    "Hazelaarweg" = "Netherlands",
    "Sportpark Het Schootsveld" = "Netherlands",
    "Sportpark Thurlede" = "Netherlands",
    "Voorburg Cricket Club" = "Netherlands",

    # ===========================================
    # AFGHANISTAN
    # ===========================================
    "Greater Noida Sports Complex Ground" = "Afghanistan",
    "Kabul" = "Afghanistan",
    "Kabul International Cricket Stadium" = "Afghanistan",

    # ===========================================
    # KENYA
    # ===========================================
    "Nairobi Gymkhana Club Ground" = "Kenya",
    "Nairobi Club" = "Kenya",
    "Sikh Union Club Ground" = "Kenya",
    "Ruaraka Sports Club Ground" = "Kenya",
    "Mombasa Sports Club Ground" = "Kenya",

    # ===========================================
    # NEPAL
    # ===========================================
    "Tribhuvan University International Cricket Ground" = "Nepal",
    "TU Cricket Ground" = "Nepal",
    "Kirtipur" = "Nepal",
    "Mulpani Cricket Ground" = "Nepal",
    "Pokhara Rangasala" = "Nepal",

    # ===========================================
    # UGANDA
    # ===========================================
    "Lugogo Cricket Oval" = "Uganda",
    "Kyambogo Cricket Oval" = "Uganda",
    "Entebbe Cricket Oval" = "Uganda",
    "Affies Park" = "Uganda",
    "Jinja" = "Uganda",

    # ===========================================
    # OMAN
    # ===========================================
    "Al Amerat Cricket Ground" = "Oman",
    "Oman Academy 1" = "Oman",
    "Oman Academy 2" = "Oman",
    "Al Amerat Cricket Stadium" = "Oman",
    "Muscat" = "Oman",

    # ===========================================
    # NAMIBIA
    # ===========================================
    "Wanderers Cricket Ground, Windhoek" = "Namibia",
    "United Ground" = "Namibia",
    "High Performance Oval, Windhoek" = "Namibia",

    # ===========================================
    # USA
    # ===========================================
    "Central Broward Regional Park Stadium Turf Ground" = "United States of America",
    "Broward County Stadium" = "United States of America",
    "Lauderhill" = "United States of America",
    "Grand Prairie Stadium" = "United States of America",
    "Prairie View Cricket Complex" = "United States of America",
    "Nassau County International Cricket Stadium" = "United States of America",

    # ===========================================
    # CANADA
    # ===========================================
    "Maple Leaf Cricket Club" = "Canada",
    "King City" = "Canada",
    "Toronto Cricket, Skating and Curling Club" = "Canada",

    # ===========================================
    # SINGAPORE
    # ===========================================
    "Indian Association Ground" = "Singapore",
    "Kallang Ground" = "Singapore",
    "Turf City" = "Singapore",

    # ===========================================
    # MALAYSIA
    # ===========================================
    "Kinrara Academy Oval" = "Malaysia",
    "UKM-YSD Cricket Oval" = "Malaysia",
    "YSD-UKM Cricket Oval" = "Malaysia",
    "Bayuemas Oval" = "Malaysia",
    "Johor Cricket Academy" = "Malaysia",
    "Royal Selangor Club" = "Malaysia",

    # ===========================================
    # HONG KONG
    # ===========================================
    "Tin Kwong Road Recreation Ground" = "Hong Kong",
    "Mong Kok" = "Hong Kong",
    "Hong Kong Cricket Club" = "Hong Kong",
    "Mission Road Ground" = "Hong Kong",
    "Happy Valley" = "Hong Kong",

    # ===========================================
    # THAILAND
    # ===========================================
    "Terdthai Cricket Ground" = "Thailand",
    "Asian Institute of Technology Ground" = "Thailand",
    "AIT Ground" = "Thailand",
    "Royal Chiang Mai Sports Club" = "Thailand",

    # ===========================================
    # PAPUA NEW GUINEA
    # ===========================================
    "Amini Park" = "Papua New Guinea",
    "Port Moresby" = "Papua New Guinea",

    # ===========================================
    # JAPAN
    # ===========================================
    "Sano International Cricket Ground" = "Japan",
    "Kaizuka Cricket Ground" = "Japan",
    "Friendship Oval" = "Japan",

    # ===========================================
    # TANZANIA
    # ===========================================
    "Gymkhana Club Ground, Dar es Salaam" = "Tanzania",
    "Dar es Salaam Gymkhana Club Ground" = "Tanzania",

    # ===========================================
    # RWANDA
    # ===========================================
    "Gahanga International Cricket Stadium" = "Rwanda",
    "Gahanga Cricket Ground" = "Rwanda",
    "Integrated Polytechnic Regional Centre Ground" = "Rwanda",

    # ===========================================
    # JERSEY
    # ===========================================
    "Grainville" = "Jersey",
    "FB Fields" = "Jersey",
    "St Clement" = "Jersey",
    "Les Quennevais" = "Jersey",

    # ===========================================
    # GUERNSEY
    # ===========================================
    "King George V Sports Ground" = "Guernsey",
    "St Peter Port" = "Guernsey",

    # ===========================================
    # BERMUDA
    # ===========================================
    "National Stadium, Hamilton" = "Bermuda",
    "White Hill Field" = "Bermuda",
    "Sandys Parish" = "Bermuda",

    # ===========================================
    # CAYMAN ISLANDS
    # ===========================================
    "Jimmy Powell Oval" = "Cayman Islands",
    "Smith Road" = "Cayman Islands",

    # ===========================================
    # FIJI
    # ===========================================
    "Albert Park" = "Fiji",

    # ===========================================
    # VANUATU
    # ===========================================
    "Independence Park" = "Vanuatu",
    "Port Vila" = "Vanuatu",

    # ===========================================
    # SAMOA
    # ===========================================
    "Faleata Oval" = "Samoa",
    "Apia" = "Samoa",

    # ===========================================
    # NIGERIA
    # ===========================================
    "Tafawa Balewa Square" = "Nigeria",
    "Lagos" = "Nigeria",
    "Abuja" = "Nigeria",

    # ===========================================
    # BOTSWANA
    # ===========================================
    "Gaborone" = "Botswana",
    "Botswana Cricket Association Ground" = "Botswana",

    # ===========================================
    # MALAWI
    # ===========================================
    "TCA Oval" = "Malawi",
    "Blantyre" = "Malawi",

    # ===========================================
    # ESWATINI
    # ===========================================
    "Malkerns" = "Eswatini",

    # ===========================================
    # ARGENTINA
    # ===========================================
    "St George's College Ground, Buenos Aires" = "Argentina",
    "Quilmes" = "Argentina",
    "Sea Breeze Oval" = "Argentina",

    # ===========================================
    # MEXICO
    # ===========================================
    "Los Reyes Polo Field" = "Mexico",
    "Naucalpan" = "Mexico",
    "Sao Fernando" = "Mexico",

    # ===========================================
    # HUNGARY
    # ===========================================
    "Szodliget Cricket Ground" = "Hungary",
    "GB Oval" = "Hungary",

    # ===========================================
    # AUSTRIA
    # ===========================================
    "Seebarn Cricket Ground" = "Austria",
    "Latschach Cricket Ground" = "Austria",
    "Velden" = "Austria",

    # ===========================================
    # BELGIUM
    # ===========================================
    "Mechelen Cricket Club" = "Belgium",
    "Zemst" = "Belgium",
    "Hofstade" = "Belgium",
    "Antwerp" = "Belgium",
    "Gent Cricket Ground" = "Belgium",

    # ===========================================
    # DENMARK
    # ===========================================
    "Brondby Stadium" = "Denmark",
    "Slagelse" = "Denmark",
    "Glostrup" = "Denmark",
    "Albertslund" = "Denmark",

    # ===========================================
    # NORWAY
    # ===========================================
    "Ekeberg Idrettspark" = "Norway",
    "Stubberudmyra" = "Norway",
    "Guttsta" = "Norway",

    # ===========================================
    # SWEDEN
    # ===========================================
    "Skarpnack" = "Sweden",
    "Botkyrka" = "Sweden",
    "Tikkurila" = "Sweden",

    # ===========================================
    # FINLAND
    # ===========================================
    "Kerava National Cricket Ground" = "Finland",

    # ===========================================
    # ITALY
    # ===========================================
    "Roma Cricket Ground" = "Italy",
    "Bologna" = "Italy",

    # ===========================================
    # SPAIN
    # ===========================================
    "Desert Springs Cricket Ground" = "Spain",
    "La Manga Club" = "Spain",
    "Almeria" = "Spain",

    # ===========================================
    # PORTUGAL
    # ===========================================
    "Santarem Cricket Ground" = "Portugal",

    # ===========================================
    # CROATIA
    # ===========================================
    "Zagreb" = "Croatia",

    # ===========================================
    # SERBIA
    # ===========================================
    "Lisicji Jarak" = "Serbia",

    # ===========================================
    # ROMANIA
    # ===========================================
    "Moara Vlasiei Cricket Ground" = "Romania",

    # ===========================================
    # CZECH REPUBLIC
    # ===========================================
    "Vinor Cricket Ground" = "Czech Republic",

    # ===========================================
    # LUXEMBOURG
    # ===========================================
    "Pierre Werner Cricket Ground" = "Luxembourg",

    # ===========================================
    # BULGARIA
    # ===========================================
    "Vassil Levski Stadium" = "Bulgaria",

    # ===========================================
    # GREECE
    # ===========================================
    "Corfu Cricket Ground" = "Greece",

    # ===========================================
    # FRANCE
    # ===========================================
    "Dreux Cricket Ground" = "France",

    # ===========================================
    # GIBRALTAR
    # ===========================================
    "Europa Sports Complex" = "Gibraltar",

    # ===========================================
    # ISLE OF MAN
    # ===========================================
    "Isle of Man Cricket Ground" = "Isle of Man",

    # ===========================================
    # BHUTAN
    # ===========================================
    "Gelephu" = "Bhutan",

    # ===========================================
    # CAMBODIA
    # ===========================================
    "Morodok Techo National Stadium" = "Cambodia",

    # ===========================================
    # INDONESIA
    # ===========================================
    "Udayana University Ground" = "Indonesia",

    # ===========================================
    # SOUTH KOREA
    # ===========================================
    "Incheon" = "South Korea",
    "Yeonhui Cricket Ground" = "South Korea",

    # ===========================================
    # QATAR
    # ===========================================
    "West End Park International Cricket Stadium" = "Qatar",
    "Doha Cricket Ground" = "Qatar",
    "Jaffery Sports Club" = "Qatar"
  )
}


#' Build Venue to Country Lookup for International Matches
#'
#' Creates a lookup table mapping venue names to country names for international matches.
#' Uses the hard-coded venue map from \code{\link{get_venue_country_map}}, with a fallback
#' to mode-based detection (team that plays there most) for unknown venues.
#'
#' @param all_matches Data frame of matches with columns: team_type, venue, team1, team2
#'
#' @return Named character vector where names are venue names and values are country names
#' @keywords internal
build_venue_country_lookup <- function(all_matches) {
  # Get unique venues from international matches
  intl_matches <- all_matches %>% dplyr::filter(.data$team_type == "international")
  if (nrow(intl_matches) == 0) return(character(0))

  unique_venues <- unique(intl_matches$venue)
  unique_venues <- unique_venues[!is.na(unique_venues) & unique_venues != ""]

  # Get the hard-coded venue map

  venue_country_map <- get_venue_country_map()

  # Pre-lowercase map keys for O(1) lookup instead of O(n) scan
  lower_map <- stats::setNames(
    as.character(venue_country_map),
    tolower(names(venue_country_map))
  )

  # Build lookup from hard-coded map
  countries <- lower_map[tolower(unique_venues)]
  names(countries) <- unique_venues
  venue_country_lookup <- countries[!is.na(countries)]

  # Fallback: for unmatched venues, use mode-based approach (team that plays there most)
  unmatched_venues <- unique_venues[is.na(countries)]

  if (length(unmatched_venues) > 0) {
    cli::cli_alert_info("Found {length(unmatched_venues)} venues not in hard-coded map, using mode fallback")

    # Find which team plays at each unmatched venue most often
    mode_venue_data <- intl_matches %>%
      dplyr::filter(.data$venue %in% unmatched_venues) %>%
      tidyr::pivot_longer(cols = c("team1", "team2"), names_to = "role", values_to = "team") %>%
      dplyr::group_by(.data$venue, .data$team) %>%
      dplyr::summarise(n_matches = dplyr::n(), .groups = "drop") %>%
      dplyr::group_by(.data$venue) %>%
      dplyr::slice_max(.data$n_matches, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup()

    # Add to lookup (mode-based fallback)
    mode_lookup <- stats::setNames(mode_venue_data$team, mode_venue_data$venue)
    venue_country_lookup <- c(venue_country_lookup, mode_lookup)
  }

  venue_country_lookup
}
