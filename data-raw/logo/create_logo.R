# Create bouncer hex sticker logo
# Produces: man/figures/logo.png

library(hexSticker)
library(ggplot2)
library(showtext)

# Use a clean sans-serif font
font_add_google("Roboto Condensed", "roboto")
showtext_auto()

# Cricket ball seam as a simple ggplot
# Draw a circle with a curved seam line
theta <- seq(0, 2 * pi, length.out = 100)
ball <- data.frame(x = cos(theta), y = sin(theta))

# Seam - a sinusoidal curve across the ball
seam_t <- seq(-0.85, 0.85, length.out = 80)
seam <- data.frame(
  x = seam_t,
  y = 0.3 * sin(seam_t * 3.5)
)

# Stitch marks along the seam
n_stitches <- 14
stitch_t <- seq(-0.75, 0.75, length.out = n_stitches)
stitch_y <- 0.3 * sin(stitch_t * 3.5)
# Small perpendicular lines for stitches
stitch_angle <- atan(0.3 * 3.5 * cos(stitch_t * 3.5))
stitch_len <- 0.08
stitches <- data.frame(
  x = stitch_t - stitch_len * sin(stitch_angle),
  xend = stitch_t + stitch_len * sin(stitch_angle),
  y = stitch_y + stitch_len * cos(stitch_angle),
  yend = stitch_y - stitch_len * cos(stitch_angle)
)

p <- ggplot() +
  # Ball body
  geom_polygon(data = ball, aes(x, y), fill = "#CC2222", colour = NA) +
  # Seam line
  geom_path(data = seam, aes(x, y), colour = "#FFDD44", linewidth = 0.8) +
  # Stitch marks
  geom_segment(data = stitches, aes(x = x, y = y, xend = xend, yend = yend),
               colour = "#FFDD44", linewidth = 0.35) +
  coord_equal(xlim = c(-1.1, 1.1), ylim = c(-1.1, 1.1)) +
  theme_void() +
  theme(plot.background = element_rect(fill = "transparent", colour = NA))

# Create hex sticker
sticker(
  p,
  package = "bouncer",
  p_size = 22,
  p_color = "#FFFFFF",
  p_family = "roboto",
  p_fontface = "bold",
  p_y = 1.45,
  s_x = 1,
  s_y = 0.85,
  s_width = 1.4,
  s_height = 1.1,
  h_fill = "#1B2838",
  h_color = "#CC2222",
  h_size = 1.5,
  url = "peteowen1.github.io/bouncer",
  u_size = 4,
  u_color = "#AAAAAA",
  filename = "man/figures/logo.png",
  dpi = 300
)

cat("Logo created at man/figures/logo.png\n")
